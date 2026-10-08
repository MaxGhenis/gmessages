package libgm

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

const testAccount = "someone@example.com"

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newTestClient(t *testing.T) (*Client, *syncBuffer) {
	t.Helper()
	logs := &syncBuffer{}
	cli := NewClient(NewAuthData(), nil, zerolog.New(logs).Level(zerolog.DebugLevel))
	cli.SetEventHandler(func(any) {})
	cli.SetResponsePayloadGrace(150 * time.Millisecond)
	return cli, logs
}

// frame describes one synthetic incoming data-route frame.
type frame struct {
	requestID   string
	action      gmproto.ActionType
	unencrypted []byte        // field 5
	payload     proto.Message // field 8 (encrypted); nil = absent
	rawPayload  []byte        // field 8 plaintext override (used when payload is nil)
	hasPayload  bool          // force field 8 presence (with rawPayload)
	account     string        // field 11 account container; "" = absent
}

func (cli *Client) encodeFrame(t testing.TB, f frame) *gmproto.IncomingRPCMessage {
	t.Helper()
	data := &gmproto.RPCMessageData{
		SessionID:       f.requestID,
		Action:          f.action,
		UnencryptedData: f.unencrypted,
	}
	if f.payload != nil || f.hasPayload {
		plaintext := f.rawPayload
		if f.payload != nil {
			var err error
			plaintext, err = proto.Marshal(f.payload)
			if err != nil {
				t.Fatal(err)
			}
		}
		encrypted, err := cli.AuthData.RequestCrypto.Encrypt(plaintext)
		if err != nil {
			t.Fatal(err)
		}
		data.EncryptedData = encrypted
	}
	if f.account != "" {
		plaintext, err := proto.Marshal(&gmproto.EncryptedData2Container{
			AccountChange: &gmproto.AccountChangeOrSomethingEvent{Account: f.account},
		})
		if err != nil {
			t.Fatal(err)
		}
		encrypted, err := cli.AuthData.RequestCrypto.Encrypt(plaintext)
		if err != nil {
			t.Fatal(err)
		}
		data.EncryptedData2 = encrypted
	}
	raw, err := proto.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	return &gmproto.IncomingRPCMessage{
		ResponseID:  "resp-" + f.requestID,
		BugleRoute:  gmproto.BugleRoute_DataEvent,
		MessageType: gmproto.MessageType_BUGLE_MESSAGE,
		MessageData: raw,
	}
}

type result struct {
	resp *IncomingRPCMessage
	err  error
}

// await registers a waiter like sendMessageWithParams does and returns a
// channel with the caller-visible result.
// await registers a waiter and waits for it through the same
// waitForResponse that sendMessageWithParams uses, so the error hand-off from
// expirePayloadWait is exercised exactly as callers see it.
func await(cli *Client, requestID string, action gmproto.ActionType) <-chan result {
	ch := cli.sessionHandler.waitResponse(requestID, action)
	out := make(chan result, 1)
	go func() {
		resp, err := cli.sessionHandler.waitForResponse(requestID, ch)
		out <- result{resp: resp, err: err}
	}()
	return out
}

func waitResult(t *testing.T, ch <-chan result) result {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(3 * time.Second):
		t.Fatal("request never completed")
		return result{}
	}
}

func assertPending(t *testing.T, ch <-chan result, d time.Duration) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("request completed early: resp=%v err=%v", r.resp != nil, r.err)
	case <-time.After(d):
	}
}

func populatedList() *gmproto.ListConversationsResponse {
	return &gmproto.ListConversationsResponse{Conversations: []*gmproto.Conversation{
		{ConversationID: "1", Name: "a"},
		{ConversationID: "2", Name: "b"},
	}}
}

// populatedFor returns a non-empty response of the type action expects.
func populatedFor(action gmproto.ActionType) proto.Message {
	conv := &gmproto.Conversation{ConversationID: "1", Name: "a"}
	switch action {
	case gmproto.ActionType_LIST_MESSAGES:
		return &gmproto.ListMessagesResponse{Messages: []*gmproto.Message{{MessageID: "m1"}}}
	case gmproto.ActionType_GET_OR_CREATE_CONVERSATION:
		return &gmproto.GetOrCreateConversationResponse{Conversation: conv}
	case gmproto.ActionType_GET_CONVERSATION:
		return &gmproto.GetConversationResponse{Conversation: conv}
	case gmproto.ActionType_GET_CONVERSATION_TYPE:
		return &gmproto.GetConversationTypeResponse{ConversationID: "1", Type: 2}
	case gmproto.ActionType_LIST_CONTACTS:
		return &gmproto.ListContactsResponse{Contacts: []*gmproto.Contact{{ContactID: "c1"}}}
	case gmproto.ActionType_LIST_TOP_CONTACTS:
		return &gmproto.ListTopContactsResponse{Contacts: []*gmproto.Contact{{ContactID: "c1"}}}
	case gmproto.ActionType_GET_PARTICIPANTS_THUMBNAIL, gmproto.ActionType_GET_CONTACTS_THUMBNAIL:
		return &gmproto.GetThumbnailResponse{Thumbnail: []*gmproto.GetThumbnailResponse_Thumbnail{{Identifier: "p1"}}}
	case gmproto.ActionType_GET_FULL_SIZE_IMAGE:
		// The schema defines no fields; real image data arrives as unknown bytes.
		m := &gmproto.GetFullSizeImageResponse{}
		raw := protowire.AppendTag(nil, 1, protowire.BytesType)
		m.ProtoReflect().SetUnknown(protowire.AppendBytes(raw, []byte{0xff, 0xd8, 0xff}))
		return m
	default:
		return populatedList()
	}
}

func TestPopulatedResponseIsDeliveredImmediately(t *testing.T) {
	cli, _ := newTestClient(t)
	ch := await(cli, "req-1", gmproto.ActionType_LIST_CONVERSATIONS)
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_LIST_CONVERSATIONS, payload: populatedList()}))
	r := waitResult(t, ch)
	if r.err != nil {
		t.Fatal(r.err)
	}
	list, err := typedResponse[*gmproto.ListConversationsResponse](r.resp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.GetConversations()) != 2 {
		t.Fatalf("got %d conversations", len(list.GetConversations()))
	}
}

// The live 2026-10-07 failure: the phone answers a pull with a frame carrying
// only the field-11 account container. That used to complete the waiter with a
// pre-allocated empty ListConversationsResponse and a nil error.
func TestAccountContainerOnlyFrameFailsInsteadOfReturningEmpty(t *testing.T) {
	cli, logs := newTestClient(t)
	ch := await(cli, "req-1", gmproto.ActionType_LIST_CONVERSATIONS)
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_LIST_CONVERSATIONS, account: testAccount}))
	assertPending(t, ch, 50*time.Millisecond)
	r := waitResult(t, ch)
	if r.resp != nil {
		t.Fatal("payload-less frame was delivered as a response")
	}
	if !errors.Is(r.err, ErrNoResponsePayload) {
		t.Fatalf("err = %v, want ErrNoResponsePayload", r.err)
	}
	var perr *ResponsePayloadError
	if !errors.As(r.err, &perr) {
		t.Fatalf("err %T is not a *ResponsePayloadError", r.err)
	}
	if !perr.AccountSwitch || perr.Frames != 1 || perr.Action != gmproto.ActionType_LIST_CONVERSATIONS {
		t.Fatalf("unexpected error detail: %+v", perr)
	}
	if !perr.Shape.HasAccountField || perr.Shape.HasPayload {
		t.Fatalf("shape lost field presence: %+v", perr.Shape)
	}
	if strings.Contains(logs.String(), testAccount) || strings.Contains(r.err.Error(), testAccount) {
		t.Fatal("the account identifier leaked into logs or the error")
	}
}

func TestHeaderOnlyFrameFails(t *testing.T) {
	cli, _ := newTestClient(t)
	ch := await(cli, "req-1", gmproto.ActionType_GET_OR_CREATE_CONVERSATION)
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_GET_OR_CREATE_CONVERSATION}))
	r := waitResult(t, ch)
	var perr *ResponsePayloadError
	if !errors.As(r.err, &perr) || perr.AccountSwitch {
		t.Fatalf("err = %v, want a ResponsePayloadError without an account switch", r.err)
	}
}

// If the payload-less frame is only an intermediate answer, the real response
// that follows within the grace period must still reach the caller.
func TestRealResponseAfterPayloadlessFrameIsDelivered(t *testing.T) {
	cli, _ := newTestClient(t)
	cli.SetResponsePayloadGrace(2 * time.Second)
	ch := await(cli, "req-1", gmproto.ActionType_LIST_CONVERSATIONS)
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_LIST_CONVERSATIONS, account: testAccount}))
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_LIST_CONVERSATIONS, account: testAccount}))
	assertPending(t, ch, 30*time.Millisecond)
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_LIST_CONVERSATIONS, payload: populatedList()}))
	r := waitResult(t, ch)
	if r.err != nil {
		t.Fatal(r.err)
	}
	if got := len(r.resp.DecryptedMessage.(*gmproto.ListConversationsResponse).GetConversations()); got != 2 {
		t.Fatalf("got %d conversations", got)
	}
	cli.sessionHandler.responseWaitersLock.Lock()
	defer cli.sessionHandler.responseWaitersLock.Unlock()
	if len(cli.sessionHandler.responseWaiters) != 0 {
		t.Fatal("waiter leaked after completion")
	}
}

// An encrypted payload that decodes to an empty message is a legitimate empty
// answer (an empty folder) and must stay a success.
func TestEncryptedEmptyPayloadIsALegitimateEmptyAnswer(t *testing.T) {
	cli, _ := newTestClient(t)
	ch := await(cli, "req-1", gmproto.ActionType_LIST_CONVERSATIONS)
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_LIST_CONVERSATIONS, hasPayload: true}))
	r := waitResult(t, ch)
	if r.err != nil {
		t.Fatal(r.err)
	}
	if !r.resp.Shape.HasPayload || !r.resp.Shape.ContentFree() {
		t.Fatalf("shape = %+v", r.resp.Shape)
	}
}

// Actions outside the payload-required set keep the original behaviour: the
// first matching frame completes the request, payload or not. The liveness
// pinger relies on this for NOTIFY_DITTO_ACTIVITY.
func TestNonDataActionsKeepFirstFrameSemantics(t *testing.T) {
	for _, action := range []gmproto.ActionType{
		gmproto.ActionType_NOTIFY_DITTO_ACTIVITY,
		gmproto.ActionType_SEND_MESSAGE,
		gmproto.ActionType_IS_BUGLE_DEFAULT,
	} {
		t.Run(action.String(), func(t *testing.T) {
			cli, _ := newTestClient(t)
			ch := await(cli, "req-1", action)
			cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: action}))
			r := waitResult(t, ch)
			if r.err != nil || r.resp == nil {
				t.Fatalf("resp=%v err=%v", r.resp != nil, r.err)
			}
		})
	}
}

func TestUnencryptedIntermediateFrameIsStillIgnored(t *testing.T) {
	cli, _ := newTestClient(t)
	ch := await(cli, "req-1", gmproto.ActionType_LIST_MESSAGES)
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_LIST_MESSAGES, unencrypted: []byte{1, 2, 3}}))
	assertPending(t, ch, 300*time.Millisecond) // longer than the grace: no timer was started
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_LIST_MESSAGES, payload: &gmproto.ListMessagesResponse{}}))
	if r := waitResult(t, ch); r.err != nil {
		t.Fatal(r.err)
	}
}

func TestFramesForOtherRequestsDoNotCompleteTheWaiter(t *testing.T) {
	cli, logs := newTestClient(t)
	ch := await(cli, "req-1", gmproto.ActionType_LIST_CONVERSATIONS)
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-2", action: gmproto.ActionType_LIST_CONVERSATIONS, payload: populatedList()}))
	assertPending(t, ch, 300*time.Millisecond)
	if !strings.Contains(logs.String(), "Received response with no pending request") {
		t.Fatal("orphan response was not logged")
	}
}

func TestShapeRecordsFieldsWithoutContent(t *testing.T) {
	cli, _ := newTestClient(t)
	list := populatedList()
	raw, _ := proto.Marshal(list)
	raw = protowire.AppendTag(raw, 99, protowire.VarintType)
	raw = protowire.AppendVarint(raw, 7)
	msg, err := cli.decryptInternalMessage(cli.encodeFrame(t, frame{
		requestID: "req-1", action: gmproto.ActionType_LIST_CONVERSATIONS,
		hasPayload: true, rawPayload: raw, unencrypted: []byte{9},
	}))
	if err != nil {
		t.Fatal(err)
	}
	s := msg.Shape
	if !s.HasPayload || !s.HasUnencrypted || s.HasAccountField || s.UnencryptedLen != 1 {
		t.Fatalf("presence wrong: %+v", s)
	}
	if s.DecryptedLen != len(raw) || s.DecodedSize != len(raw) || s.UnknownLen != 3 || s.ContentFree() {
		t.Fatalf("sizes wrong: %+v (raw %d)", s, len(raw))
	}
	if s.DecodedType != "client.ListConversationsResponse" || s.Action != gmproto.ActionType_LIST_CONVERSATIONS {
		t.Fatalf("type wrong: %+v", s)
	}
}

// Invariant, checked over random frame sequences for every payload-required
// action: the caller is completed by the first frame the acceptance rule
// accepts (any field-8 frame; a bare header for a listing), delivered as
// decoded; frames the rule rejects (the field-11 account container; a bare
// header for an object lookup) never complete it and arm the grace timer, and
// expiry then yields ErrNoResponsePayload; ignored intermediates and other
// requests' frames neither complete it nor arm the timer.
func TestAcceptanceInvariantOverRandomFrameSequences(t *testing.T) {
	rng := rand.New(rand.NewSource(20261008))
	var actions []gmproto.ActionType
	for action := range payloadRequired {
		actions = append(actions, action)
	}
	sort.Slice(actions, func(i, j int) bool { return actions[i] < actions[j] })
	kinds := []string{"header", "account", "unencrypted", "other-request", "payload", "empty-payload"}
	for iter := 0; iter < 400; iter++ {
		cli, logs := newTestClient(t)
		cli.SetResponsePayloadGrace(time.Hour) // expiry is driven explicitly below
		action := actions[rng.Intn(len(actions))]
		reqID := fmt.Sprintf("req-%d", iter)
		ch := await(cli, reqID, action)
		var seq []string
		want, rejected := "", false
		for n := rng.Intn(6); len(seq) <= n; {
			kind := kinds[rng.Intn(len(kinds))]
			seq = append(seq, kind)
			f := frame{requestID: reqID, action: action}
			switch kind {
			case "account":
				f.account = testAccount
			case "unencrypted":
				f.unencrypted = []byte{1}
			case "other-request":
				f.requestID = reqID + "-other"
				f.payload = populatedFor(action)
			case "payload":
				f.payload = populatedFor(action)
			case "empty-payload":
				f.hasPayload = true
			}
			cli.HandleRPCMsg(cli.encodeFrame(t, f))
			if want != "" {
				continue
			}
			switch {
			case kind == "payload" || kind == "empty-payload":
				want = kind
			case kind == "header" && !objectRequired[action]:
				want = kind
			case kind == "account" || kind == "header":
				rejected = true
			}
		}
		if want == "" {
			select {
			case r := <-ch:
				t.Fatalf("%s seq %v completed without an accepted frame: resp=%v err=%v", action, seq, r.resp != nil, r.err)
			case <-time.After(5 * time.Millisecond):
			}
			cli.sessionHandler.responseWaitersLock.Lock()
			w := cli.sessionHandler.responseWaiters[reqID]
			cli.sessionHandler.responseWaitersLock.Unlock()
			if w == nil {
				t.Fatalf("%s seq %v dropped the waiter", action, seq)
			}
			if armed := w.graceTimer != nil; armed != rejected {
				t.Fatalf("%s seq %v: grace timer armed=%t, want %t", action, seq, armed, rejected)
			}
			if !rejected {
				cli.sessionHandler.cancelResponse(reqID, w.ch)
				continue
			}
			w.graceTimer.Stop()
			go cli.sessionHandler.expirePayloadWait(reqID, w)
			r := waitResult(t, ch)
			if r.resp != nil || !errors.Is(r.err, ErrNoResponsePayload) {
				t.Fatalf("%s seq %v: resp=%v err=%v, want ErrNoResponsePayload", action, seq, r.resp != nil, r.err)
			}
			continue
		}
		r := waitResult(t, ch)
		if r.err != nil || r.resp == nil {
			t.Fatalf("%s seq %v: resp=%v err=%v", action, seq, r.resp != nil, r.err)
		}
		gotPayload := r.resp.Shape.HasPayload
		if gotPayload != (want != "header") || (gotPayload && (r.resp.Shape.DecryptedLen == 0) != (want == "empty-payload")) {
			t.Fatalf("%s seq %v: delivered the wrong frame (want %s): %+v", action, seq, want, r.resp.Shape)
		}
		if strings.Contains(logs.String(), testAccount) {
			t.Fatalf("%s seq %v leaked the account identifier into logs", action, seq)
		}
	}
}

// A bare header answering a listing keeps the old behaviour: it is delivered
// as an empty result, because no capture shows how a healthy phone encodes an
// empty folder.
func TestHeaderOnlyFrameStillCompletesAListing(t *testing.T) {
	cli, _ := newTestClient(t)
	ch := await(cli, "req-1", gmproto.ActionType_LIST_CONVERSATIONS)
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_LIST_CONVERSATIONS}))
	r := waitResult(t, ch)
	if r.err != nil || r.resp == nil || r.resp.Shape.HasPayload {
		t.Fatalf("resp=%v err=%v", r.resp != nil, r.err)
	}
}

// typedResponse must surface a payload error itself, so a caller that skips
// waitForResponse (or a rebase that loses its check) gets the error rather
// than a nil-pointer panic on the synthetic message.
func TestTypedResponseSurfacesPayloadError(t *testing.T) {
	perr := &ResponsePayloadError{Action: gmproto.ActionType_GET_CONVERSATION}
	got, err := typedResponse[*gmproto.GetConversationResponse](&IncomingRPCMessage{payloadErr: perr}, nil)
	if got != nil || !errors.Is(err, ErrNoResponsePayload) {
		t.Fatalf("got=%v err=%v", got, err)
	}
	if _, err := typedResponse[*gmproto.GetConversationResponse](&IncomingRPCMessage{}, nil); err == nil {
		t.Fatal("an empty synthetic message must be an error, not a success")
	}
}

// A payload-less answer proves the phone is responding, so a request still
// waiting for its real response must not poke the pinger into reporting
// PhoneNotResponding (observed live: the poke flapped responding/not
// responding and re-triggered reconciles).
func TestPayloadlessAnswerMarksRequestAnswered(t *testing.T) {
	cli, _ := newTestClient(t)
	cli.SetResponsePayloadGrace(time.Hour)
	ch := await(cli, "req-1", gmproto.ActionType_LIST_CONVERSATIONS)
	sh := cli.sessionHandler
	if sh.answeredWithoutPayload("req-1") {
		t.Fatal("unanswered request reported as answered")
	}
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_LIST_CONVERSATIONS, account: testAccount}))
	if !sh.answeredWithoutPayload("req-1") {
		t.Fatal("payload-less answer not recorded")
	}
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_LIST_CONVERSATIONS, payload: populatedList()}))
	waitResult(t, ch)
	if sh.answeredWithoutPayload("req-1") {
		t.Fatal("completed request still reported as pending")
	}
}

// Review regression (PR 193 finding 1): with trace logging on, a content-free
// read response is surfaced at Info, and that Info event must stay shape-only.
// Raw bytes stay confined to the Debug/Trace "Received response" event.
func TestContentFreeInfoEventCarriesNoPayloadBytes(t *testing.T) {
	cli, logs := newTestClient(t)
	cli.Logger = zerolog.New(logs).Level(zerolog.TraceLevel)
	const secret = "private-account@example.com"
	raw := protowire.AppendTag(nil, 99, protowire.BytesType)
	raw = protowire.AppendBytes(raw, []byte(secret))
	ch := await(cli, "privacy", gmproto.ActionType_LIST_CONVERSATIONS)
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{
		requestID: "privacy", action: gmproto.ActionType_LIST_CONVERSATIONS,
		hasPayload: true, rawPayload: raw,
	}))
	if r := waitResult(t, ch); r.err != nil {
		t.Fatal(r.err)
	}
	sawInfo := false
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		if event["level"] != "info" {
			continue
		}
		if event["message"] == "Response payload decoded to no known fields" {
			sawInfo = true
		}
		if _, ok := event["data"]; ok || strings.Contains(line, base64.StdEncoding.EncodeToString(raw)) {
			t.Fatalf("Info event carries payload bytes: %s", line)
		}
	}
	if !sawInfo {
		t.Fatal("content-free response was not surfaced at Info")
	}
}

// Review regression (PR 193 finding 4): the grace setter may race with
// response receipt; run under -race.
func TestGraceSetterIsSafeDuringReceipt(t *testing.T) {
	cli, _ := newTestClient(t)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			cli.SetResponsePayloadGrace(time.Hour + time.Duration(i))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			id := fmt.Sprintf("grace-%d", i)
			ch := cli.sessionHandler.waitResponse(id, gmproto.ActionType_LIST_MESSAGES)
			cli.sessionHandler.receiveResponse(&IncomingRPCMessage{
				IncomingRPCMessage: &gmproto.IncomingRPCMessage{ResponseID: "account-" + id},
				Message:            &gmproto.RPCMessageData{SessionID: id, Action: gmproto.ActionType_LIST_MESSAGES},
				Shape:              ResponseShape{HasAccountField: true, AccountSwitch: true},
			})
			cli.sessionHandler.cancelResponse(id, ch)
		}
	}()
	wg.Wait()
}

// Terminal ownership: expiry, a real response and cancellation racing on one
// waiter complete it at most once, never send on a closed channel, and never
// leave the waiter registered.
func TestTerminalRacesCompleteExactlyOnce(t *testing.T) {
	cli, _ := newTestClient(t)
	cli.SetResponsePayloadGrace(time.Hour)
	sh := cli.sessionHandler
	for i := 0; i < 1500; i++ {
		id := fmt.Sprintf("terminal-%d", i)
		ch := sh.waitResponse(id, gmproto.ActionType_LIST_MESSAGES)
		sh.receiveResponse(&IncomingRPCMessage{
			IncomingRPCMessage: &gmproto.IncomingRPCMessage{ResponseID: "account-" + id},
			Message:            &gmproto.RPCMessageData{SessionID: id, Action: gmproto.ActionType_LIST_MESSAGES},
			Shape:              ResponseShape{HasAccountField: true, AccountSwitch: true},
		})
		sh.responseWaitersLock.Lock()
		w := sh.responseWaiters[id]
		sh.responseWaitersLock.Unlock()
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); <-start; sh.expirePayloadWait(id, w) }()
		go func() {
			defer wg.Done()
			<-start
			sh.receiveResponse(&IncomingRPCMessage{
				IncomingRPCMessage: &gmproto.IncomingRPCMessage{ResponseID: "payload-" + id},
				Message:            &gmproto.RPCMessageData{SessionID: id, Action: gmproto.ActionType_LIST_MESSAGES},
				Shape:              ResponseShape{HasPayload: true},
			})
		}()
		go func() { defer wg.Done(); <-start; sh.cancelResponse(id, ch) }()
		close(start)
		wg.Wait()
		w.graceTimer.Stop()
		sh.responseWaitersLock.Lock()
		_, pending := sh.responseWaiters[id]
		sh.responseWaitersLock.Unlock()
		if pending {
			t.Fatalf("iteration %d leaked the waiter", i)
		}
		select {
		case _, open := <-ch:
			if open {
				select {
				case dup, stillOpen := <-ch:
					if stillOpen || dup != nil {
						t.Fatalf("iteration %d completed twice", i)
					}
				default:
				}
			}
		default:
			t.Fatalf("iteration %d neither completed nor cancelled", i)
		}
	}
}

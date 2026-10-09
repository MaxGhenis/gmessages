package libgm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/pblite"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/events"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

// pendingWaiters reports how many requests are still registered.
func pendingWaiters(cli *Client) int {
	cli.sessionHandler.responseWaitersLock.Lock()
	defer cli.sessionHandler.responseWaitersLock.Unlock()
	return len(cli.sessionHandler.responseWaiters)
}

func TestUnansweredRequestFailsWithPhoneNotResponding(t *testing.T) {
	cli, logs := newTestClient(t)
	cli.SetRequestTimeout(100 * time.Millisecond)
	start := time.Now()
	ch := await(cli, "req-1", gmproto.ActionType_LIST_MESSAGES)
	r := waitResult(t, ch)
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("request failed after %s, before its timeout", elapsed)
	}
	if r.resp != nil || !errors.Is(r.err, ErrPhoneNotResponding) {
		t.Fatalf("resp=%v err=%v, want ErrPhoneNotResponding", r.resp != nil, r.err)
	}
	if errors.Is(r.err, ErrConnectionClosed) || errors.Is(r.err, ErrNoResponsePayload) {
		t.Fatalf("timeout matches another sentinel: %v", r.err)
	}
	var uerr *UnansweredRequestError
	if !errors.As(r.err, &uerr) || uerr.Action != gmproto.ActionType_LIST_MESSAGES || uerr.Waited != 100*time.Millisecond {
		t.Fatalf("unexpected error detail: %#v", r.err)
	}
	if want := "LIST_MESSAGES: phone did not respond to request after 100ms"; r.err.Error() != want {
		t.Fatalf("error text %q, want %q", r.err.Error(), want)
	}
	if n := pendingWaiters(cli); n != 0 {
		t.Fatalf("%d waiter(s) left registered after the timeout", n)
	}
	if !strings.Contains(logs.String(), "Phone did not answer the request in time") {
		t.Fatal("timeout was not logged")
	}
}

func TestAnswerBeforeTimeoutIsDelivered(t *testing.T) {
	cli, _ := newTestClient(t)
	cli.SetRequestTimeout(2 * time.Second)
	ch := await(cli, "req-1", gmproto.ActionType_LIST_CONVERSATIONS)
	assertPending(t, ch, 50*time.Millisecond)
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_LIST_CONVERSATIONS, payload: populatedList()}))
	r := waitResult(t, ch)
	if r.err != nil {
		t.Fatal(r.err)
	}
	if got := len(r.resp.DecryptedMessage.(*gmproto.ListConversationsResponse).GetConversations()); got != 2 {
		t.Fatalf("got %d conversations", got)
	}
}

// An answer that arrives after its request timed out has nobody to go to: it
// is logged and dropped, never sent on the closed channel.
func TestLateAnswerAfterTimeoutIsDropped(t *testing.T) {
	cli, logs := newTestClient(t)
	cli.SetRequestTimeout(50 * time.Millisecond)
	ch := await(cli, "req-1", gmproto.ActionType_LIST_CONVERSATIONS)
	if r := waitResult(t, ch); !errors.Is(r.err, ErrPhoneNotResponding) {
		t.Fatalf("err = %v", r.err)
	}
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_LIST_CONVERSATIONS, payload: populatedList()}))
	if !strings.Contains(logs.String(), "Received response with no pending request") {
		t.Fatal("late answer was not logged as unclaimed")
	}
}

// A request the phone answered only without a payload, cut off by the
// timeout before the payload grace ran out, reports the payload error: it is
// the more specific cause, and it carries the account-switch notice that
// callers act on.
func TestTimeoutAfterPayloadlessAnswerReportsPayloadError(t *testing.T) {
	cli, _ := newTestClient(t)
	cli.SetResponsePayloadGrace(time.Hour)
	cli.SetRequestTimeout(100 * time.Millisecond)
	ch := await(cli, "req-1", gmproto.ActionType_LIST_CONVERSATIONS)
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_LIST_CONVERSATIONS, account: testAccount}))
	r := waitResult(t, ch)
	var perr *ResponsePayloadError
	if !errors.As(r.err, &perr) || !perr.AccountSwitch || perr.Frames != 1 {
		t.Fatalf("err = %v, want an account-switch ResponsePayloadError", r.err)
	}
	if errors.Is(r.err, ErrPhoneNotResponding) {
		t.Fatal("a phone that answered was reported as not responding")
	}
	if n := pendingWaiters(cli); n != 0 {
		t.Fatalf("%d waiter(s) left registered", n)
	}
}

func TestRequestTimeoutSetting(t *testing.T) {
	cli, _ := newTestClient(t)
	if got := cli.RequestTimeout(); got != DefaultRequestTimeout {
		t.Fatalf("default = %s, want %s", got, DefaultRequestTimeout)
	}
	if DefaultRequestTimeout < time.Minute || DefaultRequestTimeout > 2*time.Minute {
		t.Fatalf("DefaultRequestTimeout %s is outside 1-2 minutes", DefaultRequestTimeout)
	}
	cli.SetRequestTimeout(90 * time.Second)
	if got := cli.RequestTimeout(); got != 90*time.Second {
		t.Fatalf("after set = %s", got)
	}
	for _, reset := range []time.Duration{0, -time.Second} {
		cli.SetRequestTimeout(time.Second)
		cli.SetRequestTimeout(reset)
		if got := cli.RequestTimeout(); got != DefaultRequestTimeout {
			t.Fatalf("SetRequestTimeout(%s) left %s, want the default", reset, got)
		}
	}
}

// A request keeps the timeout it started with.
func TestInFlightRequestKeepsItsTimeout(t *testing.T) {
	cli, _ := newTestClient(t)
	cli.SetRequestTimeout(150 * time.Millisecond)
	ch := await(cli, "req-1", gmproto.ActionType_LIST_MESSAGES)
	time.Sleep(20 * time.Millisecond) // let the wait start and read its timeout
	cli.SetRequestTimeout(time.Hour)
	r := waitResult(t, ch)
	var uerr *UnansweredRequestError
	if !errors.As(r.err, &uerr) || uerr.Waited != 150*time.Millisecond {
		t.Fatalf("err = %v", r.err)
	}
}

// Disconnect fails every waiting request at once, whatever its action and
// whether or not it is inside a payload grace period.
func TestDisconnectFailsPendingRequests(t *testing.T) {
	cli, _ := newTestClient(t)
	cli.SetRequestTimeout(time.Hour)
	cli.SetResponsePayloadGrace(300 * time.Millisecond)
	actions := []gmproto.ActionType{
		gmproto.ActionType_LIST_CONVERSATIONS,
		gmproto.ActionType_LIST_MESSAGES,
		gmproto.ActionType_SEND_MESSAGE,
		gmproto.ActionType_GET_OR_CREATE_CONVERSATION,
	}
	var waits []<-chan result
	for i, action := range actions {
		waits = append(waits, await(cli, fmt.Sprintf("req-%d", i), action))
	}
	// One request is inside its payload grace period when the client drops.
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-0", action: actions[0], account: testAccount}))
	for _, ch := range waits {
		assertPending(t, ch, 10*time.Millisecond)
	}
	start := time.Now()
	cli.Disconnect()
	for i, ch := range waits {
		r := waitResult(t, ch)
		if r.resp != nil || !errors.Is(r.err, ErrConnectionClosed) {
			t.Fatalf("%s: resp=%v err=%v, want ErrConnectionClosed", actions[i], r.resp != nil, r.err)
		}
		var uerr *UnansweredRequestError
		if !errors.As(r.err, &uerr) || uerr.Action != actions[i] {
			t.Fatalf("%s: unexpected error detail %#v", actions[i], r.err)
		}
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("requests took %s to fail after Disconnect", elapsed)
	}
	if n := pendingWaiters(cli); n != 0 {
		t.Fatalf("%d waiter(s) left registered after Disconnect", n)
	}
	// The stopped grace timer must not fire into the failed request later.
	time.Sleep(400 * time.Millisecond)
}

func TestRequestsAfterDisconnectStillWork(t *testing.T) {
	cli, _ := newTestClient(t)
	cli.Disconnect()
	ch := await(cli, "req-1", gmproto.ActionType_LIST_CONVERSATIONS)
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_LIST_CONVERSATIONS, payload: populatedList()}))
	if r := waitResult(t, ch); r.err != nil || r.resp == nil {
		t.Fatalf("resp=%v err=%v", r.resp != nil, r.err)
	}
}

// outcome is what the caller of a request saw.
type outcome string

const (
	outcomeResponse     outcome = "response"
	outcomeNoPayload    outcome = "no-payload"
	outcomeNotResponded outcome = "not-responding"
	outcomeClosed       outcome = "connection-closed"
	outcomePending      outcome = "pending"
)

func classifyOutcome(t *testing.T, resp *IncomingRPCMessage, err error) outcome {
	t.Helper()
	switch {
	case err == nil && resp != nil:
		return outcomeResponse
	case errors.Is(err, ErrNoResponsePayload):
		return outcomeNoPayload
	case errors.Is(err, ErrPhoneNotResponding):
		return outcomeNotResponded
	case errors.Is(err, ErrConnectionClosed):
		return outcomeClosed
	default:
		t.Fatalf("unclassifiable result resp=%v err=%v", resp != nil, err)
		return ""
	}
}

// Invariant, over random event sequences for payload-required and
// first-frame actions: a request ends exactly once, with the outcome of the
// first terminal event (an accepted frame; the payload-grace expiry after a
// rejected frame; a disconnect; the timeout, which reports the payload error
// if the phone answered without one). Every later event is a no-op that
// neither panics nor changes the result, and no waiter stays registered.
func TestTerminalOutcomeIsTheFirstTerminalEvent(t *testing.T) {
	rng := rand.New(rand.NewSource(20261009))
	actions := []gmproto.ActionType{
		gmproto.ActionType_LIST_CONVERSATIONS,
		gmproto.ActionType_LIST_MESSAGES,
		gmproto.ActionType_GET_OR_CREATE_CONVERSATION,
		gmproto.ActionType_GET_CONVERSATION,
		gmproto.ActionType_SEND_MESSAGE,
		gmproto.ActionType_NOTIFY_DITTO_ACTIVITY,
	}
	kinds := []string{"payload", "header", "account", "other-request", "expire", "disconnect", "timeout"}
	seen := map[outcome]int{}
	for iter := 0; iter < 2000; iter++ {
		cli, _ := newTestClient(t)
		cli.Logger = zerolog.Nop()
		cli.SetResponsePayloadGrace(time.Hour) // expiry is driven explicitly
		sh := cli.sessionHandler
		action := actions[rng.Intn(len(actions))]
		reqID := fmt.Sprintf("req-%d", iter)
		ch := sh.waitResponse(reqID, action)
		sh.responseWaitersLock.Lock()
		w := sh.responseWaiters[reqID]
		sh.responseWaitersLock.Unlock()
		start := time.Now()

		want := outcomePending
		rejected := false
		var got outcome
		var seq []string
		for n := 1 + rng.Intn(6); len(seq) < n; {
			kind := kinds[rng.Intn(len(kinds))]
			seq = append(seq, kind)
			f := frame{requestID: reqID, action: action}
			switch kind {
			case "payload":
				f.payload = populatedFor(action)
				cli.HandleRPCMsg(cli.encodeFrame(t, f))
			case "header":
				cli.HandleRPCMsg(cli.encodeFrame(t, f))
			case "account":
				f.account = testAccount
				cli.HandleRPCMsg(cli.encodeFrame(t, f))
			case "other-request":
				f.requestID = reqID + "-other"
				f.payload = populatedFor(action)
				cli.HandleRPCMsg(cli.encodeFrame(t, f))
			case "expire":
				// The grace timer exists only once a frame was rejected.
				if rejected {
					sh.expirePayloadWait(reqID, w)
				}
			case "disconnect":
				cli.Disconnect()
			case "timeout":
				if got == "" {
					resp, err := sh.abandonRequest(reqID, action, ch, time.Minute, start)
					got = classifyOutcome(t, resp, err)
				}
			}
			if want != outcomePending {
				continue
			}
			switch kind {
			case "payload":
				want = outcomeResponse
			case "header":
				if rejectsPayloadless(action, ResponseShape{}) {
					rejected = true
				} else {
					want = outcomeResponse
				}
			case "account":
				if rejectsPayloadless(action, ResponseShape{HasAccountField: true}) {
					rejected = true
				} else {
					want = outcomeResponse
				}
			case "expire":
				if rejected {
					want = outcomeNoPayload
				}
			case "disconnect":
				want = outcomeClosed
			case "timeout":
				want = outcomeNotResponded
				if rejected {
					want = outcomeNoPayload
				}
			}
		}
		if got == "" {
			select {
			case resp, ok := <-ch:
				final, err := terminalResponse(resp, ok, action, start)
				got = classifyOutcome(t, final, err)
			default:
				got = outcomePending
				if sh.cancelResponse(reqID, ch) == nil {
					t.Fatalf("%s seq %v: pending request had no registered waiter", action, seq)
				}
			}
		}
		if got != want {
			t.Fatalf("%s seq %v: outcome %s, want %s", action, seq, got, want)
		}
		if n := pendingWaiters(cli); n != 0 {
			t.Fatalf("%s seq %v: %d waiter(s) left registered", action, seq, n)
		}
		// Nothing else may arrive on the channel after the terminal outcome.
		select {
		case extra, open := <-ch:
			if open {
				t.Fatalf("%s seq %v: a second terminal message arrived: %v", action, seq, extra != nil)
			}
		default:
		}
		seen[want]++
	}
	for _, o := range []outcome{outcomeResponse, outcomeNoPayload, outcomeNotResponded, outcomeClosed, outcomePending} {
		if seen[o] == 0 {
			t.Errorf("no sequence produced outcome %s; the generator lost coverage", o)
		}
	}
}

// The timeout, a response, a payload-wait expiry and a disconnect racing on
// one waiter end it exactly once, never send on a closed channel and never
// leave it registered. Run under -race.
func TestTimeoutRacesCompleteExactlyOnce(t *testing.T) {
	cli, _ := newTestClient(t)
	cli.Logger = zerolog.Nop()
	cli.SetResponsePayloadGrace(time.Hour)
	sh := cli.sessionHandler
	counts := map[outcome]int{}
	for i := 0; i < 1500; i++ {
		id := fmt.Sprintf("race-%d", i)
		action := gmproto.ActionType_LIST_MESSAGES
		ch := sh.waitResponse(id, action)
		sh.receiveResponse(&IncomingRPCMessage{
			IncomingRPCMessage: &gmproto.IncomingRPCMessage{ResponseID: "account-" + id},
			Message:            &gmproto.RPCMessageData{SessionID: id, Action: action},
			Shape:              ResponseShape{HasAccountField: true, AccountSwitch: true},
		})
		sh.responseWaitersLock.Lock()
		w := sh.responseWaiters[id]
		sh.responseWaitersLock.Unlock()
		start := make(chan struct{})
		var wg sync.WaitGroup
		var resp *IncomingRPCMessage
		var err error
		wg.Add(4)
		go func() {
			defer wg.Done()
			<-start
			resp, err = sh.abandonRequest(id, action, ch, time.Minute, time.Now())
		}()
		go func() { defer wg.Done(); <-start; sh.expirePayloadWait(id, w) }()
		go func() {
			defer wg.Done()
			<-start
			sh.receiveResponse(&IncomingRPCMessage{
				IncomingRPCMessage: &gmproto.IncomingRPCMessage{ResponseID: "payload-" + id},
				Message:            &gmproto.RPCMessageData{SessionID: id, Action: action},
				Shape:              ResponseShape{HasPayload: true},
			})
		}()
		go func() { defer wg.Done(); <-start; sh.failPendingRequests() }()
		close(start)
		wg.Wait()
		w.graceTimer.Stop()
		counts[classifyOutcome(t, resp, err)]++
		if n := pendingWaiters(cli); n != 0 {
			t.Fatalf("iteration %d left %d waiter(s) registered", i, n)
		}
		select {
		case extra, open := <-ch:
			if open {
				t.Fatalf("iteration %d completed twice (%v)", i, extra != nil)
			}
		default:
		}
	}
	t.Logf("outcomes: %v", counts)
}

// stubTransport answers every libgm HTTP request with an empty success and
// hands each sent request to onSend, so tests can drive the public methods
// without a network.
type stubTransport struct {
	onSend func(*gmproto.OutgoingRPCMessage)
}

func (s stubTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	var sent gmproto.OutgoingRPCMessage
	if err := pblite.Unmarshal(body, &sent); err != nil {
		return nil, err
	}
	if s.onSend != nil {
		s.onSend(&sent)
	}
	respBody, err := proto.Marshal(&gmproto.OutgoingRPCResponse{})
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {ContentTypeProtobuf}},
		Body:       io.NopCloser(bytes.NewReader(respBody)),
		Request:    req,
	}, nil
}

func useStubTransport(cli *Client, onSend func(*gmproto.OutgoingRPCMessage)) {
	cli.http = &http.Client{Transport: stubTransport{onSend: onSend}}
}

// The public methods time out end to end: the request is sent, the phone
// never answers, and the caller gets ErrPhoneNotResponding instead of
// blocking forever.
func TestPublicMethodsTimeOut(t *testing.T) {
	cli, _ := newTestClient(t)
	cli.SetRequestTimeout(150 * time.Millisecond)
	var sent atomic.Int32
	useStubTransport(cli, func(*gmproto.OutgoingRPCMessage) { sent.Add(1) })

	calls := map[string]func() error{
		"ListConversations": func() error {
			_, err := cli.ListConversations(10, gmproto.ListConversationsRequest_INBOX)
			return err
		},
		"FetchMessages": func() error {
			_, err := cli.FetchMessages("conv", 10, nil)
			return err
		},
		"GetConversation": func() error {
			_, err := cli.GetConversation("conv")
			return err
		},
		"SendMessage": func() error {
			_, err := cli.SendMessage(&gmproto.SendMessageRequest{ConversationID: "conv", TmpID: "tmp"})
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() { done <- call() }()
			select {
			case err := <-done:
				if !errors.Is(err, ErrPhoneNotResponding) {
					t.Fatalf("err = %v, want ErrPhoneNotResponding", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("call still blocked long after its timeout")
			}
		})
	}
	if int(sent.Load()) != len(calls) {
		t.Fatalf("%d request(s) sent, want %d", sent.Load(), len(calls))
	}
	if n := pendingWaiters(cli); n != 0 {
		t.Fatalf("%d waiter(s) left registered", n)
	}
}

// The end-to-end path still delivers a real answer.
func TestPublicMethodDeliversAnswer(t *testing.T) {
	cli, _ := newTestClient(t)
	cli.SetRequestTimeout(2 * time.Second)
	useStubTransport(cli, func(sent *gmproto.OutgoingRPCMessage) {
		cli.HandleRPCMsg(cli.encodeFrame(t, frame{
			requestID: sent.GetData().GetRequestID(),
			action:    gmproto.ActionType_LIST_CONVERSATIONS,
			payload:   populatedList(),
		}))
	})
	resp, err := cli.ListConversations(10, gmproto.ListConversationsRequest_INBOX)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetConversations()) != 2 {
		t.Fatalf("got %d conversations", len(resp.GetConversations()))
	}
}

// A public call blocked on an unanswered request returns as soon as the
// client disconnects.
func TestPublicMethodFailsOnDisconnect(t *testing.T) {
	cli, _ := newTestClient(t)
	cli.SetRequestTimeout(time.Hour)
	sentCh := make(chan struct{}, 1)
	useStubTransport(cli, func(*gmproto.OutgoingRPCMessage) { sentCh <- struct{}{} })
	done := make(chan error, 1)
	go func() {
		_, err := cli.FetchMessages("conv", 10, nil)
		done <- err
	}()
	<-sentCh
	cli.Disconnect()
	select {
	case err := <-done:
		if !errors.Is(err, ErrConnectionClosed) {
			t.Fatalf("err = %v, want ErrConnectionClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("call still blocked after Disconnect")
	}
}

// A ping whose channel is closed (the client disconnected) is not an answer:
// it must not tell listeners the phone is responding again.
func TestPingerIgnoresAbandonedPing(t *testing.T) {
	for _, answered := range []bool{false, true} {
		t.Run(fmt.Sprintf("answered=%t", answered), func(t *testing.T) {
			cli, _ := newTestClient(t)
			var responding atomic.Int32
			cli.SetEventHandler(func(evt any) {
				if _, ok := evt.(*events.PhoneRespondingAgain); ok {
					responding.Add(1)
				}
			})
			log := zerolog.Nop()
			dp := &dittoPinger{client: cli, stop: make(chan struct{}), log: &log, notRespondingSent: true}
			pingChan := make(chan *IncomingRPCMessage, 1)
			if answered {
				pingChan <- &IncomingRPCMessage{}
			} else {
				close(pingChan)
			}
			dp.WaitForResponse(1, time.Now(), time.Hour, 0, pingChan, newResetter())
			if got := responding.Load() == 1; got != answered {
				t.Fatalf("PhoneRespondingAgain fired=%t, want %t", got, answered)
			}
		})
	}
}

// Once a ping's wait ends, its waiter is removed rather than kept until the
// next disconnect.
func TestPingWaiterIsRemovedAfterTheWait(t *testing.T) {
	cli, _ := newTestClient(t)
	useStubTransport(cli, nil)
	stop := make(chan struct{})
	close(stop) // the wait ends at once, unanswered
	log := zerolog.Nop()
	dp := &dittoPinger{client: cli, stop: stop, log: &log}
	dp.Ping(1, time.Hour, 0, newResetter())
	if n := pendingWaiters(cli); n != 0 {
		t.Fatalf("%d ping waiter(s) left registered", n)
	}
}

// Google-account pairing messages wait on the raw channel: a disconnect must
// fail them with ErrConnectionClosed instead of a nil-pointer panic, and a
// cancelled context must not leave the waiter registered.
func TestGaiaPairingMessageEndsCleanly(t *testing.T) {
	t.Run("disconnect", func(t *testing.T) {
		cli, _ := newTestClient(t)
		sentCh := make(chan struct{}, 1)
		useStubTransport(cli, func(*gmproto.OutgoingRPCMessage) { sentCh <- struct{}{} })
		done := make(chan error, 1)
		go func() {
			_, err := cli.sendGaiaPairingMessage(context.Background(), &PairingSession{}, gmproto.ActionType_CREATE_GAIA_PAIRING_CLIENT_INIT, nil)
			done <- err
		}()
		<-sentCh
		cli.Disconnect()
		select {
		case err := <-done:
			if !errors.Is(err, ErrConnectionClosed) {
				t.Fatalf("err = %v, want ErrConnectionClosed", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("pairing message still blocked after Disconnect")
		}
	})
	t.Run("context", func(t *testing.T) {
		cli, _ := newTestClient(t)
		useStubTransport(cli, nil)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := cli.sendGaiaPairingMessage(ctx, &PairingSession{}, gmproto.ActionType_CREATE_GAIA_PAIRING_CLIENT_INIT, nil)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v", err)
		}
		if n := pendingWaiters(cli); n != 0 {
			t.Fatalf("%d waiter(s) left registered", n)
		}
	})
}

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
	"testing/synctest"
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

// An answer that claims the waiter as the timeout fires wins: the caller gets
// the answer, not ErrPhoneNotResponding, and the answer is not dropped.
// abandonRequest is what waitForResponse runs when its timer fires; here the
// answer is received first, as when it lands between the timer firing and the
// waiter's removal.
func TestAnswerAtTheDeadlineWinsOverTheTimeout(t *testing.T) {
	cli, logs := newTestClient(t)
	sh := cli.sessionHandler
	action := gmproto.ActionType_LIST_CONVERSATIONS
	ch := sh.waitResponse("req-1", action)
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: action, payload: populatedList()}))
	resp, err := sh.abandonRequest("req-1", action, ch, time.Minute, time.Now())
	if err != nil || resp == nil {
		t.Fatalf("resp=%v err=%v, want the answer", resp != nil, err)
	}
	if got := len(resp.DecryptedMessage.(*gmproto.ListConversationsResponse).GetConversations()); got != 2 {
		t.Fatalf("got %d conversations", got)
	}
	if n := pendingWaiters(cli); n != 0 {
		t.Fatalf("%d waiter(s) left registered", n)
	}
	if strings.Contains(logs.String(), "Phone did not answer the request in time") {
		t.Fatal("an answered request was logged as timed out")
	}
}

// The ownership rule behind exactly-once: whoever removes a waiter from the
// map owns the single send on its channel. A timeout that finds the waiter
// already claimed waits for that send rather than reporting a timeout or
// returning without a result.
func TestTimeoutWaitsForTheClaimerToDeliver(t *testing.T) {
	cli, _ := newTestClient(t)
	sh := cli.sessionHandler
	action := gmproto.ActionType_LIST_CONVERSATIONS
	ch := sh.waitResponse("req-1", action)
	// Claim the waiter the way receiveResponse does, without sending yet.
	sh.responseWaitersLock.Lock()
	delete(sh.responseWaiters, "req-1")
	sh.responseWaitersLock.Unlock()
	done := make(chan result, 1)
	go func() {
		resp, err := sh.abandonRequest("req-1", action, ch, time.Minute, time.Now())
		done <- result{resp: resp, err: err}
	}()
	assertPending(t, done, 50*time.Millisecond)
	answer := &IncomingRPCMessage{IncomingRPCMessage: &gmproto.IncomingRPCMessage{ResponseID: "late-but-claimed"}}
	ch <- answer
	r := waitResult(t, done)
	if r.err != nil || r.resp != answer {
		t.Fatalf("resp=%v err=%v, want the claimer's answer", r.resp, r.err)
	}
}

// Request IDs are random, but a reused one must not orphan the earlier
// waiter: it fails at once (as a disconnect would, with its grace timer
// stopped) instead of waiting on a channel nobody owns, the earlier owner's
// cancel can't remove the later waiter, and the later request still gets its
// answer.
func TestReusedRequestIDFailsTheEarlierWaiter(t *testing.T) {
	cli, logs := newTestClient(t)
	cli.SetRequestTimeout(time.Hour)
	cli.SetResponsePayloadGrace(time.Hour)
	sh := cli.sessionHandler
	action := gmproto.ActionType_LIST_CONVERSATIONS
	first := sh.waitResponse("req-1", action)
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: action, account: testAccount}))
	grace := graceTimerOf(t, cli, "req-1")
	second := await(cli, "req-1", action)
	select {
	case resp, ok := <-first:
		if ok {
			t.Fatalf("earlier waiter got %v, want its channel closed", resp)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("earlier waiter still blocked after its ID was reused")
	}
	if grace.Stop() {
		t.Fatal("reuse left the earlier waiter's grace timer running")
	}
	if !strings.Contains(logs.String(), "Request ID reused") {
		t.Fatal("reuse was not logged")
	}
	if sh.cancelResponse("req-1", first) != nil || pendingWaiters(cli) != 1 {
		t.Fatal("cancelling the earlier waiter removed the later one")
	}
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: action, payload: populatedList()}))
	if r := waitResult(t, second); r.err != nil || r.resp == nil {
		t.Fatalf("later waiter: resp=%v err=%v", r.resp != nil, r.err)
	}
	if n := pendingWaiters(cli); n != 0 {
		t.Fatalf("%d waiter(s) left registered", n)
	}
}

// A request the phone answered only without a payload, cut off by the
// timeout before the payload grace ran out, reports the payload error: it is
// the more specific cause, and it carries the account-switch notice that
// callers act on.
func TestTimeoutAfterPayloadlessAnswerReportsPayloadError(t *testing.T) {
	// Fake time: the frame is handled before the timeout can fire, however
	// loaded the machine.
	synctest.Test(t, func(t *testing.T) {
		cli, _ := newTestClient(t)
		cli.SetResponsePayloadGrace(time.Hour)
		cli.SetRequestTimeout(100 * time.Millisecond)
		ch := await(cli, "req-1", gmproto.ActionType_LIST_CONVERSATIONS)
		cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_LIST_CONVERSATIONS, account: testAccount}))
		grace := graceTimerOf(t, cli, "req-1")
		r := <-ch
		if grace.Stop() {
			t.Fatal("the timeout left the payload grace timer running")
		}
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
	})
}

// graceTimerOf returns the payload grace timer of a pending request.
func graceTimerOf(t *testing.T, cli *Client, requestID string) *time.Timer {
	t.Helper()
	cli.sessionHandler.responseWaitersLock.Lock()
	defer cli.sessionHandler.responseWaitersLock.Unlock()
	w := cli.sessionHandler.responseWaiters[requestID]
	if w == nil || w.graceTimer == nil {
		t.Fatalf("precondition: %s should be inside its payload grace period", requestID)
	}
	return w.graceTimer
}

// A real answer after a payload-less one is delivered and stops the grace
// timer, so it can't fire into the finished request.
func TestAnswerAfterPayloadlessFrameStopsTheGraceTimer(t *testing.T) {
	cli, _ := newTestClient(t)
	cli.SetResponsePayloadGrace(time.Hour)
	ch := await(cli, "req-1", gmproto.ActionType_LIST_CONVERSATIONS)
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_LIST_CONVERSATIONS, account: testAccount}))
	grace := graceTimerOf(t, cli, "req-1")
	cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_LIST_CONVERSATIONS, payload: populatedList()}))
	if r := waitResult(t, ch); r.err != nil || r.resp == nil {
		t.Fatalf("resp=%v err=%v", r.resp != nil, r.err)
	}
	if grace.Stop() {
		t.Fatal("the answer left the payload grace timer running")
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
	synctest.Test(t, func(t *testing.T) {
		cli, _ := newTestClient(t)
		cli.SetRequestTimeout(150 * time.Millisecond)
		start := time.Now()
		ch := await(cli, "req-1", gmproto.ActionType_LIST_MESSAGES)
		synctest.Wait() // the wait has started and read its timeout
		cli.SetRequestTimeout(time.Hour)
		r := <-ch
		var uerr *UnansweredRequestError
		if !errors.As(r.err, &uerr) || uerr.Waited != 150*time.Millisecond {
			t.Fatalf("err = %v", r.err)
		}
		if waited := time.Since(start); waited != 150*time.Millisecond {
			t.Fatalf("request ended after %s, want 150ms", waited)
		}
	})
}

// The 5 s nudge pokes the liveness pinger but does not end the wait: an
// answer after it is delivered. With the default timeout, a request the
// phone never answers fails at exactly DefaultRequestTimeout.
func TestNudgePokesThePingerWithoutEndingTheWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cli, _ := newTestClient(t)
		cli.Logger = zerolog.Nop()
		poked := make(chan struct{}, 1)
		go func() {
			<-cli.pingShortCircuit
			poked <- struct{}{}
		}()
		ch := await(cli, "req-1", gmproto.ActionType_LIST_CONVERSATIONS)
		time.Sleep(phoneNotRespondingNudge + time.Second)
		synctest.Wait()
		select {
		case <-poked:
		default:
			t.Fatal("the nudge did not poke the pinger")
		}
		select {
		case r := <-ch:
			t.Fatalf("request ended at the nudge: err=%v", r.err)
		default:
		}
		cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: "req-1", action: gmproto.ActionType_LIST_CONVERSATIONS, payload: populatedList()}))
		if r := <-ch; r.err != nil || r.resp == nil {
			t.Fatalf("answer after the nudge was not delivered: %v", r.err)
		}

		start := time.Now()
		r := <-await(cli, "req-2", gmproto.ActionType_LIST_CONVERSATIONS)
		if !errors.Is(r.err, ErrPhoneNotResponding) {
			t.Fatalf("err = %v, want ErrPhoneNotResponding", r.err)
		}
		if waited := time.Since(start); waited != DefaultRequestTimeout {
			t.Fatalf("default timeout fired after %s, want %s", waited, DefaultRequestTimeout)
		}
	})
}

// Disconnect fails every waiting request at once, whatever its action and
// whether or not it is inside a payload grace period.
func TestDisconnectFailsPendingRequests(t *testing.T) {
	cli, _ := newTestClient(t)
	cli.SetRequestTimeout(time.Hour)
	cli.SetResponsePayloadGrace(time.Hour)
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
	cli.sessionHandler.responseWaitersLock.Lock()
	grace := cli.sessionHandler.responseWaiters["req-0"].graceTimer
	cli.sessionHandler.responseWaitersLock.Unlock()
	if grace == nil {
		t.Fatal("precondition: req-0 should be inside its payload grace period")
	}
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
	// Disconnect stopped the grace timer, so it can't fire into the failed
	// request later. Stop reports whether the timer was still running.
	if grace.Stop() {
		t.Fatal("Disconnect left the payload grace timer running")
	}
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

// modelRejectsPayloadless is the model's own statement of which payload-less
// frames must not complete a request, written out here rather than computed
// with the production predicate, so a wrong predicate can't make the model
// agree with it. Every payload-required read rejects a frame holding only the
// account container; lookups that must return one object also reject a bare
// header; sends and pings take the first frame.
var modelRejectsPayloadless = map[gmproto.ActionType]struct{ header, account bool }{
	gmproto.ActionType_LIST_CONVERSATIONS:         {header: false, account: true},
	gmproto.ActionType_LIST_MESSAGES:              {header: false, account: true},
	gmproto.ActionType_GET_OR_CREATE_CONVERSATION: {header: true, account: true},
	gmproto.ActionType_GET_CONVERSATION:           {header: true, account: true},
	gmproto.ActionType_SEND_MESSAGE:               {header: false, account: false},
	gmproto.ActionType_NOTIFY_DITTO_ACTIVITY:      {header: false, account: false},
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
	for _, action := range actions {
		if _, ok := modelRejectsPayloadless[action]; !ok {
			t.Fatalf("modelRejectsPayloadless has no entry for %s", action)
		}
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
				// The grace timer exists only once a frame was rejected. Stop
				// it before expiring by hand, so it can't fire an hour later
				// and keep this iteration's client alive until then.
				if rejected {
					sh.responseWaitersLock.Lock()
					timer := w.graceTimer
					sh.responseWaitersLock.Unlock()
					if timer != nil {
						timer.Stop()
					}
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
				if modelRejectsPayloadless[action].header {
					rejected = true
				} else {
					want = outcomeResponse
				}
			case "account":
				if modelRejectsPayloadless[action].account {
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

// The same race without a payload-less frame first: a plain timeout against
// a response and a disconnect on one waiter. Run under -race.
func TestPlainTimeoutRacesCompleteExactlyOnce(t *testing.T) {
	cli, _ := newTestClient(t)
	cli.Logger = zerolog.Nop()
	sh := cli.sessionHandler
	counts := map[outcome]int{}
	for i := 0; i < 1500; i++ {
		id := fmt.Sprintf("plain-%d", i)
		action := gmproto.ActionType_SEND_MESSAGE
		ch := sh.waitResponse(id, action)
		start := make(chan struct{})
		var wg sync.WaitGroup
		var resp *IncomingRPCMessage
		var err error
		wg.Add(3)
		go func() {
			defer wg.Done()
			<-start
			resp, err = sh.abandonRequest(id, action, ch, time.Minute, time.Now())
		}()
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
	if counts[outcomeNoPayload] != 0 {
		t.Fatalf("a request with no payload-less frame reported a payload error: %v", counts)
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

// publicCall is a public method that waits for the phone's answer: the
// action it sends, an answer of the type it expects, and what the method
// returns for that answer.
type publicCall struct {
	name   string
	action gmproto.ActionType
	answer proto.Message
	want   proto.Message
	call   func(cli *Client) (proto.Message, error)
}

func publicCalls() []publicCall {
	conv := &gmproto.Conversation{ConversationID: "1", Name: "a"}
	sent := &gmproto.SendMessageResponse{Status: gmproto.SendMessageResponse_SUCCESS}
	messages := populatedFor(gmproto.ActionType_LIST_MESSAGES)
	return []publicCall{{
		name:   "ListConversations",
		action: gmproto.ActionType_LIST_CONVERSATIONS,
		answer: populatedList(),
		want:   populatedList(),
		call: func(cli *Client) (proto.Message, error) {
			return cli.ListConversations(10, gmproto.ListConversationsRequest_INBOX)
		},
	}, {
		name:   "FetchMessages",
		action: gmproto.ActionType_LIST_MESSAGES,
		answer: messages,
		want:   messages,
		call: func(cli *Client) (proto.Message, error) {
			return cli.FetchMessages("conv", 10, nil)
		},
	}, {
		name:   "GetConversation",
		action: gmproto.ActionType_GET_CONVERSATION,
		answer: &gmproto.GetConversationResponse{Conversation: conv},
		want:   conv,
		call: func(cli *Client) (proto.Message, error) {
			return cli.GetConversation("conv")
		},
	}, {
		name:   "SendMessage",
		action: gmproto.ActionType_SEND_MESSAGE,
		answer: sent,
		want:   sent,
		call: func(cli *Client) (proto.Message, error) {
			return cli.SendMessage(&gmproto.SendMessageRequest{ConversationID: "conv", TmpID: "tmp"})
		},
	}}
}

// runCall runs c in the background and returns its result, failing the test
// if it is still blocked after limit.
func runCall(t *testing.T, cli *Client, c publicCall, limit time.Duration) (proto.Message, error) {
	t.Helper()
	type callResult struct {
		msg proto.Message
		err error
	}
	done := make(chan callResult, 1)
	go func() {
		msg, err := c.call(cli)
		done <- callResult{msg, err}
	}()
	select {
	case r := <-done:
		return r.msg, r.err
	case <-time.After(limit):
		t.Fatalf("%s still blocked after %s", c.name, limit)
		return nil, nil
	}
}

// The public methods time out end to end: the request is sent, the phone
// never answers, and the caller gets ErrPhoneNotResponding instead of
// blocking forever.
func TestPublicMethodsTimeOut(t *testing.T) {
	for _, c := range publicCalls() {
		t.Run(c.name, func(t *testing.T) {
			cli, _ := newTestClient(t)
			cli.SetRequestTimeout(150 * time.Millisecond)
			var sent atomic.Int32
			useStubTransport(cli, func(*gmproto.OutgoingRPCMessage) { sent.Add(1) })
			_, err := runCall(t, cli, c, 3*time.Second)
			if !errors.Is(err, ErrPhoneNotResponding) {
				t.Fatalf("err = %v, want ErrPhoneNotResponding", err)
			}
			var uerr *UnansweredRequestError
			if !errors.As(err, &uerr) || uerr.Action != c.action {
				t.Fatalf("unexpected error detail %#v", err)
			}
			if sent.Load() != 1 {
				t.Fatalf("%d request(s) sent, want 1", sent.Load())
			}
			if n := pendingWaiters(cli); n != 0 {
				t.Fatalf("%d waiter(s) left registered", n)
			}
		})
	}
}

// The end-to-end path still delivers a real answer.
func TestPublicMethodsDeliverTheAnswer(t *testing.T) {
	for _, c := range publicCalls() {
		t.Run(c.name, func(t *testing.T) {
			cli, _ := newTestClient(t)
			cli.SetRequestTimeout(2 * time.Second)
			useStubTransport(cli, func(sent *gmproto.OutgoingRPCMessage) {
				cli.HandleRPCMsg(cli.encodeFrame(t, frame{
					requestID: sent.GetData().GetRequestID(),
					action:    c.action,
					payload:   c.answer,
				}))
			})
			got, err := runCall(t, cli, c, 3*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(got, c.want) {
				t.Fatalf("got %v, want %v", got, c.want)
			}
			if n := pendingWaiters(cli); n != 0 {
				t.Fatalf("%d waiter(s) left registered", n)
			}
		})
	}
}

// A public call blocked on an unanswered request returns as soon as the
// client disconnects.
func TestPublicMethodsFailOnDisconnect(t *testing.T) {
	for _, c := range publicCalls() {
		t.Run(c.name, func(t *testing.T) {
			cli, _ := newTestClient(t)
			cli.SetRequestTimeout(time.Hour)
			sentCh := make(chan struct{}, 1)
			useStubTransport(cli, func(*gmproto.OutgoingRPCMessage) { sentCh <- struct{}{} })
			go func() {
				<-sentCh
				cli.Disconnect()
			}()
			_, err := runCall(t, cli, c, 3*time.Second)
			if !errors.Is(err, ErrConnectionClosed) {
				t.Fatalf("err = %v, want ErrConnectionClosed", err)
			}
			if n := pendingWaiters(cli); n != 0 {
				t.Fatalf("%d waiter(s) left registered", n)
			}
		})
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

// The same holds once a ping has outlived its timeout and the pinger is in
// its backoff loop: a closed channel there is not an answer either.
func TestPingerIgnoresAbandonedPingDuringBackoff(t *testing.T) {
	for _, answered := range []bool{false, true} {
		t.Run(fmt.Sprintf("answered=%t", answered), func(t *testing.T) {
			cli, _ := newTestClient(t)
			var responding atomic.Int32
			timedOut := make(chan struct{}, 1)
			cli.SetEventHandler(func(evt any) {
				switch evt.(type) {
				case *events.PhoneNotResponding:
					select {
					case timedOut <- struct{}{}:
					default:
					}
				case *events.PhoneRespondingAgain:
					responding.Add(1)
				}
			})
			log := zerolog.Nop()
			dp := &dittoPinger{client: cli, stop: make(chan struct{}), log: &log}
			pingChan := make(chan *IncomingRPCMessage, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				// timeoutCount 1 starts no reping ticker, so after the timeout
				// the backoff loop only waits on the ping's channel.
				dp.WaitForResponse(1, time.Now(), time.Millisecond, 1, pingChan, newResetter())
			}()
			select {
			case <-timedOut: // OnTimeout ran: the wait is in the backoff loop
			case <-time.After(3 * time.Second):
				t.Fatal("ping never timed out")
			}
			if answered {
				pingChan <- &IncomingRPCMessage{}
			} else {
				close(pingChan)
			}
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("backoff loop kept waiting after the ping's channel yielded")
			}
			if got := responding.Load() == 1; got != answered {
				t.Fatalf("PhoneRespondingAgain fired=%t, want %t", got, answered)
			}
		})
	}
}

// Through Ping() itself, not just its wait: an unanswered ping reports the
// phone as not responding, and an answer that arrives once the wait has
// started reports it responding again. Moving the waiter's removal ahead of
// the wait would silently stop both.
func TestPingReportsUnansweredAndAnsweredPings(t *testing.T) {
	t.Run("unanswered", func(t *testing.T) {
		cli, _ := newTestClient(t)
		useStubTransport(cli, nil)
		notResponding := make(chan struct{}, 1)
		cli.SetEventHandler(func(evt any) {
			if _, ok := evt.(*events.PhoneNotResponding); ok {
				select {
				case notResponding <- struct{}{}:
				default:
				}
			}
		})
		log := zerolog.Nop()
		stop := make(chan struct{})
		dp := &dittoPinger{client: cli, stop: stop, log: &log}
		done := make(chan struct{})
		go func() { defer close(done); dp.Ping(1, 20*time.Millisecond, 0, newResetter()) }()
		select {
		case <-notResponding:
		case <-done:
			t.Fatal("Ping returned without reporting the unanswered ping")
		case <-time.After(3 * time.Second):
			t.Fatal("no PhoneNotResponding")
		}
		close(stop)
		<-done
		if n := pendingWaiters(cli); n != 0 {
			t.Fatalf("%d ping waiter(s) left registered", n)
		}
	})
	t.Run("answered once waiting", func(t *testing.T) {
		// Fake time, so the answer lands after the wait has started.
		synctest.Test(t, func(t *testing.T) {
			cli, _ := newTestClient(t)
			sent := make(chan string, 1)
			useStubTransport(cli, func(m *gmproto.OutgoingRPCMessage) { sent <- m.GetData().GetRequestID() })
			var responding atomic.Int32
			cli.SetEventHandler(func(evt any) {
				if _, ok := evt.(*events.PhoneRespondingAgain); ok {
					responding.Add(1)
				}
			})
			log := zerolog.Nop()
			dp := &dittoPinger{client: cli, stop: make(chan struct{}), log: &log, notRespondingSent: true}
			go func() {
				id := <-sent
				time.Sleep(50 * time.Millisecond)
				cli.HandleRPCMsg(cli.encodeFrame(t, frame{requestID: id, action: gmproto.ActionType_NOTIFY_DITTO_ACTIVITY}))
			}()
			dp.Ping(1, time.Hour, 0, newResetter())
			if responding.Load() != 1 {
				t.Fatal("an answered ping did not report PhoneRespondingAgain")
			}
			// The resetter closes its channel 5 s after the answer; let it.
			time.Sleep(6 * time.Second)
		})
	})
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

package libgm

import (
	"errors"
	"fmt"
	"time"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

// ErrPhoneNotResponding is returned when the phone doesn't answer a request
// within the request timeout (SetRequestTimeout). The server had already
// accepted the request, so the phone may still act on it if it comes back
// online: a send that fails this way may yet be delivered.
//
// The name and text match upstream mautrix-gmessages, which added the same
// timeout in c0a2d38, so callers keep working across a rebase onto it.
var ErrPhoneNotResponding = errors.New("phone did not respond to request")

// ErrConnectionClosed is returned for a request still waiting for its
// response when the client disconnects (Client.Disconnect). Responses arrive
// over the long-polling connection, so none can arrive once it is torn down.
var ErrConnectionClosed = errors.New("client disconnected before response was received")

// DefaultRequestTimeout is how long a request waits for the phone's answer,
// once sent, before failing with ErrPhoneNotResponding. It matches upstream's
// responseHardTimeout.
const DefaultRequestTimeout = 60 * time.Second

// phoneNotRespondingNudge is how long a request waits before poking the
// liveness pinger, which reports PhoneNotResponding if its own ping goes
// unanswered too.
const phoneNotRespondingNudge = 5 * time.Second

// UnansweredRequestError reports a request that ended without an answer:
// the phone did not respond within the request timeout (Reason
// ErrPhoneNotResponding), or the client disconnected while it waited (Reason
// ErrConnectionClosed). errors.Is matches Reason.
type UnansweredRequestError struct {
	// Action is the request's action.
	Action gmproto.ActionType
	// Reason is ErrPhoneNotResponding or ErrConnectionClosed.
	Reason error
	// Waited is how long the request waited for an answer after it was sent.
	Waited time.Duration
}

func (e *UnansweredRequestError) Error() string {
	return fmt.Sprintf("%s: %v after %s", e.Action, e.Reason, e.Waited.Round(time.Millisecond))
}

func (e *UnansweredRequestError) Unwrap() error {
	return e.Reason
}

// SetRequestTimeout sets how long each request waits for the phone's answer
// before failing with ErrPhoneNotResponding. A timeout <= 0 restores
// DefaultRequestTimeout. Requests already waiting keep the timeout they
// started with.
func (c *Client) SetRequestTimeout(timeout time.Duration) {
	if timeout < 0 {
		timeout = 0
	}
	c.sessionHandler.requestTimeout.Store(int64(timeout))
}

// RequestTimeout reports how long each request waits for the phone's answer.
func (c *Client) RequestTimeout() time.Duration {
	return c.sessionHandler.requestTimeoutDuration()
}

func (s *SessionHandler) requestTimeoutDuration() time.Duration {
	if timeout := time.Duration(s.requestTimeout.Load()); timeout > 0 {
		return timeout
	}
	return DefaultRequestTimeout
}

// timeoutError is the error for a waiter whose request timed out. A request
// the phone already answered without a payload reports that (it is the more
// specific cause, and it carries the account-switch notice) rather than a
// silent phone.
func (w *responseWaiter) timeoutError(timeout time.Duration) error {
	if w.skipped > 0 {
		return w.payloadError()
	}
	return &UnansweredRequestError{Action: w.action, Reason: ErrPhoneNotResponding, Waited: timeout}
}

// failPendingRequests fails every request still waiting for a response by
// removing its waiter and closing its channel, which the waiting caller
// reports as ErrConnectionClosed. Each waiter has exactly one owner (see
// cancelResponse), so a response, expiry or timeout racing with this either
// claimed the waiter first or finds it gone.
func (s *SessionHandler) failPendingRequests() int {
	s.responseWaitersLock.Lock()
	defer s.responseWaitersLock.Unlock()
	failed := len(s.responseWaiters)
	for requestID, w := range s.responseWaiters {
		if w.graceTimer != nil {
			w.graceTimer.Stop()
		}
		delete(s.responseWaiters, requestID)
		close(w.ch)
	}
	return failed
}

package libgm

import (
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

// ErrNoResponsePayload is returned when the phone answers a data request only
// with frames that carry no response payload (no encrypted field 8): a
// header-only frame, or a frame holding just the field-11 account container.
// Such a frame cannot contain the requested data, so treating it as a
// successful empty response would silently hide the failure.
var ErrNoResponsePayload = errors.New("phone answered without a response payload")

// DefaultResponsePayloadGrace is how long a request keeps waiting for its real
// response after the phone has answered it with a payload-less frame.
const DefaultResponsePayloadGrace = 10 * time.Second

// payloadRequired lists the read actions whose answer only ever arrives in the
// encrypted response payload (RPCMessageData field 8). Actions outside this set
// (sends, liveness pings, updates) keep the original first-matching-frame
// behaviour.
var payloadRequired = map[gmproto.ActionType]bool{
	gmproto.ActionType_LIST_CONVERSATIONS:         true,
	gmproto.ActionType_LIST_MESSAGES:              true,
	gmproto.ActionType_GET_OR_CREATE_CONVERSATION: true,
	gmproto.ActionType_GET_CONVERSATION:           true,
	gmproto.ActionType_GET_CONVERSATION_TYPE:      true,
	gmproto.ActionType_LIST_CONTACTS:              true,
	gmproto.ActionType_LIST_TOP_CONTACTS:          true,
	gmproto.ActionType_GET_PARTICIPANTS_THUMBNAIL: true,
	gmproto.ActionType_GET_CONTACTS_THUMBNAIL:     true,
	gmproto.ActionType_GET_FULL_SIZE_IMAGE:        true,
}

// objectRequired lists the payload-required lookups that must return one
// object (a conversation, its type, an image), so an empty answer is never a
// legitimate result for them. Listings can legitimately be empty.
var objectRequired = map[gmproto.ActionType]bool{
	gmproto.ActionType_GET_OR_CREATE_CONVERSATION: true,
	gmproto.ActionType_GET_CONVERSATION:           true,
	gmproto.ActionType_GET_CONVERSATION_TYPE:      true,
	gmproto.ActionType_GET_FULL_SIZE_IMAGE:        true,
}

// rejectsPayloadless reports whether a frame without the encrypted payload
// must not complete a request for action. Two kinds of payload-less frame
// are rejected:
//
//   - one carrying the field-11 account container, which is how a phone that
//     switched to Google-account pairing answers a QR browser's requests
//     (observed 2026-10-08); it never holds the requested data;
//   - a bare header (no field 5, 8 or 11) answering a lookup that must
//     return an object.
//
// A bare header answering a listing is still delivered as an empty result: no
// healthy-phone capture shows how an empty folder is encoded, so this keeps
// the old behaviour there rather than turning a possibly legitimate empty
// answer into an error. Field-5-only intermediate frames never reach this
// check (receiveResponse ignores them for HasCookies sessions).
func rejectsPayloadless(action gmproto.ActionType, shape ResponseShape) bool {
	if shape.HasPayload || !payloadRequired[action] {
		return false
	}
	if shape.HasAccountField {
		return true
	}
	return objectRequired[action] && !shape.HasUnencrypted
}

// ResponsePayloadRequired reports whether a response to action must carry the
// encrypted payload to be accepted.
func ResponsePayloadRequired(action gmproto.ActionType) bool {
	return payloadRequired[action]
}

// ResponseShape describes the envelope of an incoming data-route frame without
// carrying any of its content, so request/response acceptance can be logged
// and validated without exposing message data or account identifiers.
type ResponseShape struct {
	Route       gmproto.BugleRoute
	Action      gmproto.ActionType
	MessageType gmproto.MessageType

	// Field presence and byte lengths in RPCMessageData.
	HasUnencrypted  bool // field 5
	UnencryptedLen  int
	HasPayload      bool // field 8: the encrypted response payload
	PayloadLen      int
	HasAccountField bool // field 11: the account-container side channel
	AccountFieldLen int
	Flags           [3]bool // bool1 (6), bool2 (7), bool3 (9)

	// DecryptedLen is the plaintext length of whichever encrypted field was
	// decrypted (8 if present, else 11).
	DecryptedLen int
	// DecodedType is the response message selected by Action; DecodedSize is
	// its serialized size and UnknownLen the bytes of top-level fields this
	// schema does not know. DecodedSize == UnknownLen means no known field
	// was populated.
	DecodedType string
	DecodedSize int
	UnknownLen  int

	// GDittoSource is set on some intermediate frames (IncomingRPCMessage 23).
	GDittoSource bool
	// AccountSwitch is set when the field-11 container named a Google account,
	// which the phone sends after switching to Google-account pairing.
	AccountSwitch bool
}

// ContentFree reports whether the decoded response populated no known field.
func (s ResponseShape) ContentFree() bool {
	return s.DecodedSize == s.UnknownLen
}

func (s ResponseShape) MarshalZerologObject(e *zerolog.Event) {
	e.Stringer("route", s.Route).
		Stringer("action", s.Action).
		Stringer("message_type", s.MessageType).
		Bool("f5", s.HasUnencrypted).
		Int("f5_len", s.UnencryptedLen).
		Bool("f8", s.HasPayload).
		Int("f8_len", s.PayloadLen).
		Bool("f11", s.HasAccountField).
		Int("f11_len", s.AccountFieldLen).
		Bool("b1", s.Flags[0]).
		Bool("b2", s.Flags[1]).
		Bool("b3", s.Flags[2]).
		Int("decrypted_len", s.DecryptedLen).
		Str("decoded_type", s.DecodedType).
		Int("decoded_size", s.DecodedSize).
		Int("unknown_len", s.UnknownLen).
		Bool("gditto_source", s.GDittoSource).
		Bool("account_switch", s.AccountSwitch)
}

func (s ResponseShape) String() string {
	return fmt.Sprintf("action=%s type=%s f5=%d f8=%t/%d f11=%t/%d decoded=%s size=%d unknown=%d account_switch=%t",
		s.Action, s.MessageType, s.UnencryptedLen, s.HasPayload, s.PayloadLen,
		s.HasAccountField, s.AccountFieldLen, s.DecodedType, s.DecodedSize, s.UnknownLen, s.AccountSwitch)
}

// responseShape summarises a decoded data-route frame. It must be called after
// decryptInternalMessage has populated the message.
func responseShape(msg *IncomingRPCMessage) ResponseShape {
	shape := ResponseShape{
		Route:        msg.GetBugleRoute(),
		MessageType:  msg.GetMessageType(),
		GDittoSource: msg.GetGdittoSource() != nil,
		DecryptedLen: len(msg.DecryptedData),
	}
	if data := msg.Message; data != nil {
		shape.Action = data.GetAction()
		shape.HasUnencrypted = data.UnencryptedData != nil
		shape.UnencryptedLen = len(data.UnencryptedData)
		shape.HasPayload = data.EncryptedData != nil
		shape.PayloadLen = len(data.EncryptedData)
		shape.HasAccountField = data.EncryptedData2 != nil
		shape.AccountFieldLen = len(data.EncryptedData2)
		shape.Flags = [3]bool{data.GetBool1(), data.GetBool2(), data.GetBool3()}
	}
	if msg.DecryptedMessage != nil {
		shape.DecodedType = string(msg.DecryptedMessage.ProtoReflect().Descriptor().FullName())
		shape.DecodedSize = proto.Size(msg.DecryptedMessage)
		shape.UnknownLen = len(msg.DecryptedMessage.ProtoReflect().GetUnknown())
	}
	return shape
}

// ResponsePayloadError reports a data request the phone answered only with
// payload-less frames. It matches ErrNoResponsePayload under errors.Is.
type ResponsePayloadError struct {
	// Action is the request's action.
	Action gmproto.ActionType
	// Frames counts the payload-less frames that answered the request.
	Frames int
	// Shape describes the last of those frames.
	Shape ResponseShape
	// AccountSwitch is set when any answering frame carried a Google-account
	// switch notice in the field-11 container.
	AccountSwitch bool
	// Waited is how long the request waited from send to giving up.
	Waited time.Duration
}

func (e *ResponsePayloadError) Error() string {
	reason := "no response payload"
	if e.AccountSwitch {
		reason = "no response payload; the phone sent a Google-account switch notice instead"
	}
	return fmt.Sprintf("%s: %s after %d frame(s) in %s (%s)",
		e.Action, reason, e.Frames, e.Waited.Round(time.Millisecond), e.Shape)
}

func (e *ResponsePayloadError) Is(target error) bool {
	return target == ErrNoResponsePayload
}

// responseWaiter tracks one outstanding request.
type responseWaiter struct {
	ch     chan *IncomingRPCMessage
	action gmproto.ActionType
	sentAt time.Time

	// Payload-less answers seen so far, for payload-required actions.
	skipped       int
	lastSkipped   ResponseShape
	accountSwitch bool
	graceTimer    *time.Timer
}

// SetResponsePayloadGrace overrides how long a payload-required request keeps
// waiting for its real response after a payload-less answer.
func (c *Client) SetResponsePayloadGrace(grace time.Duration) {
	if grace > 0 {
		c.sessionHandler.payloadGrace.Store(int64(grace))
	}
}

func (s *SessionHandler) responsePayloadGrace() time.Duration {
	if grace := time.Duration(s.payloadGrace.Load()); grace > 0 {
		return grace
	}
	return DefaultResponsePayloadGrace
}

// expirePayloadWait fails a request whose only answers were payload-less once
// the grace period passes without the real response.
func (s *SessionHandler) expirePayloadWait(requestID string, w *responseWaiter) {
	s.responseWaitersLock.Lock()
	if s.responseWaiters[requestID] != w {
		s.responseWaitersLock.Unlock()
		return
	}
	delete(s.responseWaiters, requestID)
	err := w.payloadError()
	s.responseWaitersLock.Unlock()
	s.client.Logger.Warn().
		Str("request_message_id", requestID).
		Stringer("request_action", w.action).
		Int("payloadless_frames", w.skipped).
		Bool("account_switch", w.accountSwitch).
		Dur("waited", err.Waited).
		Object("last_frame", w.lastSkipped).
		Msg("Phone never sent a response payload; failing the request instead of returning an empty response")
	w.ch <- &IncomingRPCMessage{payloadErr: err}
}

// payloadError describes a request answered only by payload-less frames. The
// caller must own w: hold responseWaitersLock, or have removed w from the
// waiter map under it.
func (w *responseWaiter) payloadError() *ResponsePayloadError {
	return &ResponsePayloadError{
		Action:        w.action,
		Frames:        w.skipped,
		Shape:         w.lastSkipped,
		AccountSwitch: w.accountSwitch,
		Waited:        time.Since(w.sentAt),
	}
}

// answeredWithoutPayload reports whether a still-pending request has already
// been answered by a payload-less frame.
func (s *SessionHandler) answeredWithoutPayload(requestID string) bool {
	s.responseWaitersLock.Lock()
	defer s.responseWaitersLock.Unlock()
	w, ok := s.responseWaiters[requestID]
	return ok && w.skipped > 0
}

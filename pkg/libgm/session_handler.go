package libgm

import (
	"encoding/base64"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/util"
)

type SessionHandler struct {
	client *Client

	responseWaiters     map[string]*responseWaiter
	responseWaitersLock sync.Mutex
	payloadGrace        atomic.Int64 // time.Duration; 0 = DefaultResponsePayloadGrace

	ackMapLock sync.Mutex
	ackMap     []string
	ackTicker  *time.Ticker

	sessionID string
}

func (s *SessionHandler) ResetSessionID() {
	s.sessionID = uuid.NewString()
}

func (s *SessionHandler) sendMessageNoResponse(params SendMessageParams) error {
	requestID, payload, err := s.buildMessage(params)
	if err != nil {
		return err
	}

	url := util.SendMessageURL
	if s.client.AuthData.HasCookies() {
		url = util.SendMessageURLGoogle
	}
	s.client.Logger.Debug().
		Stringer("message_action", params.Action).
		Str("message_id", requestID).
		Msg("Sending request to phone (not expecting response)")
	_, err = typedHTTPResponse[*gmproto.OutgoingRPCResponse](
		s.client.makeProtobufHTTPRequest(url, payload, ContentTypePBLite),
	)
	return err
}

func (s *SessionHandler) sendAsyncMessage(params SendMessageParams) (<-chan *IncomingRPCMessage, error) {
	_, ch, err := s.sendAsyncMessageWithID(params)
	return ch, err
}

func (s *SessionHandler) sendAsyncMessageWithID(params SendMessageParams) (string, <-chan *IncomingRPCMessage, error) {
	requestID, payload, err := s.buildMessage(params)
	if err != nil {
		return "", nil, err
	}

	ch := s.waitResponse(requestID, params.Action)
	url := util.SendMessageURL
	if s.client.AuthData.HasCookies() {
		url = util.SendMessageURLGoogle
	}
	s.client.Logger.Debug().
		Stringer("message_action", params.Action).
		Str("message_id", requestID).
		Msg("Sending request to phone")
	_, err = typedHTTPResponse[*gmproto.OutgoingRPCResponse](
		s.client.makeProtobufHTTPRequest(url, payload, ContentTypePBLite),
	)
	if err != nil {
		s.cancelResponse(requestID, ch)
		return "", nil, err
	}
	return requestID, ch, nil
}

func typedResponse[T proto.Message](resp *IncomingRPCMessage, err error) (casted T, retErr error) {
	if err != nil {
		retErr = err
		return
	}
	if resp == nil {
		retErr = fmt.Errorf("no response received")
		return
	}
	if resp.payloadErr != nil {
		retErr = resp.payloadErr
		return
	}
	var ok bool
	casted, ok = resp.DecryptedMessage.(T)
	if !ok {
		retErr = fmt.Errorf("unexpected response type %T for %s, expected %T", resp.DecryptedMessage, resp.GetResponseID(), casted)
	}
	return
}

func (s *SessionHandler) waitResponse(requestID string, action gmproto.ActionType) chan *IncomingRPCMessage {
	ch := make(chan *IncomingRPCMessage, 1)
	s.responseWaitersLock.Lock()
	s.responseWaiters[requestID] = &responseWaiter{ch: ch, action: action, sentAt: time.Now()}
	s.responseWaitersLock.Unlock()
	return ch
}

func (s *SessionHandler) cancelResponse(requestID string, ch chan *IncomingRPCMessage) {
	s.responseWaitersLock.Lock()
	defer s.responseWaitersLock.Unlock()
	w, ok := s.responseWaiters[requestID]
	if !ok || w.ch != ch {
		// A response (or payload-wait expiry) already claimed the waiter and
		// owns the single send on ch, so it must not be closed under it.
		return
	}
	if w.graceTimer != nil {
		w.graceTimer.Stop()
	}
	delete(s.responseWaiters, requestID)
	close(ch)
}

func (s *SessionHandler) receiveResponse(msg *IncomingRPCMessage) bool {
	if msg.Message == nil {
		return false
	}
	requestID := msg.Message.SessionID
	if s.client.AuthData.HasCookies() {
		switch msg.Message.Action {
		case gmproto.ActionType_CREATE_GAIA_PAIRING_CLIENT_INIT, gmproto.ActionType_CREATE_GAIA_PAIRING_CLIENT_FINISHED:
		default:
			// Very hacky way to ignore weird messages that come before real responses
			// TODO figure out how to properly handle these
			if msg.Message.UnencryptedData != nil && msg.Message.EncryptedData == nil {
				s.responseWaitersLock.Lock()
				w, pending := s.responseWaiters[requestID]
				s.responseWaitersLock.Unlock()
				if pending && payloadRequired[w.action] {
					s.client.Logger.Info().
						Str("request_message_id", requestID).
						Stringer("request_action", w.action).
						Dur("elapsed", time.Since(w.sentAt)).
						Object("frame", msg.Shape).
						Msg("Ignoring unencrypted intermediate frame for pending request")
				}
				return false
			}
		}
	}
	s.responseWaitersLock.Lock()
	w, ok := s.responseWaiters[requestID]
	if !ok {
		s.responseWaitersLock.Unlock()
		if msg.Message.Action != gmproto.ActionType_GET_UPDATES {
			// A response nobody is waiting for: a late answer after its request
			// already completed (or failed), or a duplicate.
			s.client.Logger.Info().
				Str("request_message_id", requestID).
				Str("response_message_id", msg.ResponseID).
				Object("frame", msg.Shape).
				Msg("Received response with no pending request")
		}
		return false
	}
	if rejectsPayloadless(w.action, msg.Shape) {
		// The phone answered, but without the encrypted payload that carries
		// the requested data. Keep waiting for the real response for a bounded
		// time instead of handing the caller a pre-allocated empty message.
		w.skipped++
		w.lastSkipped = msg.Shape
		w.accountSwitch = w.accountSwitch || msg.Shape.AccountSwitch
		if w.graceTimer == nil {
			w.graceTimer = time.AfterFunc(s.responsePayloadGrace(), func() {
				s.expirePayloadWait(requestID, w)
			})
		}
		skipped := w.skipped
		s.responseWaitersLock.Unlock()
		s.client.Logger.Warn().
			Str("request_message_id", requestID).
			Str("response_message_id", msg.ResponseID).
			Stringer("request_action", w.action).
			Int("payloadless_frames", skipped).
			Dur("elapsed", time.Since(w.sentAt)).
			Object("frame", msg.Shape).
			Msg("Phone answered a pending request without a response payload; waiting for the real response")
		return true
	}
	delete(s.responseWaiters, requestID)
	if w.graceTimer != nil {
		w.graceTimer.Stop()
	}
	s.responseWaitersLock.Unlock()
	if msg.Shape.HasPayload && msg.Shape.ContentFree() && payloadRequired[w.action] &&
		msg.DecryptedMessage != nil && msg.DecryptedMessage.ProtoReflect().Descriptor().Fields().Len() > 0 {
		// Legitimate for an empty folder, but worth seeing when a pull
		// unexpectedly comes back empty. Shape only: never payload bytes.
		s.client.Logger.Info().
			Str("request_message_id", requestID).
			Stringer("request_action", w.action).
			Dur("elapsed", time.Since(w.sentAt)).
			Object("frame", msg.Shape).
			Msg("Response payload decoded to no known fields")
	}
	evt := s.client.Logger.Debug()
	evt.Str("request_message_id", requestID).
		Str("response_message_id", msg.ResponseID).
		Stringer("request_action", w.action).
		Dur("elapsed", time.Since(w.sentAt)).
		Int("payloadless_frames_before", w.skipped).
		Object("frame", msg.Shape)
	if s.client.Logger.GetLevel() == zerolog.TraceLevel {
		if msg.DecryptedData != nil {
			evt.Str("data", base64.StdEncoding.EncodeToString(msg.DecryptedData))
		}
	}
	evt.Msg("Received response")
	w.ch <- msg
	return true
}

func (s *SessionHandler) sendMessageWithParams(params SendMessageParams) (*IncomingRPCMessage, error) {
	requestID, ch, err := s.sendAsyncMessageWithID(params)
	if err != nil {
		return nil, err
	}
	return s.waitForResponse(requestID, ch)
}

// waitForResponse waits for the request's terminal message: a response, or
// the payload error that expirePayloadWait delivers.
func (s *SessionHandler) waitForResponse(requestID string, ch <-chan *IncomingRPCMessage) (*IncomingRPCMessage, error) {
	var resp *IncomingRPCMessage
	select {
	case resp = <-ch:
	case <-time.After(5 * time.Second):
		// Notify the pinger in order to trigger an event that the phone isn't
		// responding - unless the phone already answered this request without
		// a payload, which proves it is responding.
		if !s.answeredWithoutPayload(requestID) {
			select {
			case s.client.pingShortCircuit <- struct{}{}:
			default:
			}
		}
		// TODO hard timeout?
		resp = <-ch
	}
	if resp != nil && resp.payloadErr != nil {
		return nil, resp.payloadErr
	}
	return resp, nil
}

func (s *SessionHandler) sendMessage(actionType gmproto.ActionType, encryptedData proto.Message) (*IncomingRPCMessage, error) {
	return s.sendMessageWithParams(SendMessageParams{
		Action: actionType,
		Data:   encryptedData,
	})
}

type SendMessageParams struct {
	Action gmproto.ActionType
	Data   proto.Message

	RequestID   string
	OmitTTL     bool
	CustomTTL   int64
	DontEncrypt bool
	MessageType gmproto.MessageType
}

func (s *SessionHandler) buildMessage(params SendMessageParams) (string, proto.Message, error) {
	var err error
	sessionID := s.client.sessionHandler.sessionID

	requestID := params.RequestID
	if requestID == "" {
		requestID = uuid.NewString()
	}

	if params.MessageType == 0 {
		params.MessageType = gmproto.MessageType_BUGLE_MESSAGE
	}

	message := &gmproto.OutgoingRPCMessage{
		Mobile: s.client.AuthData.Mobile,
		Data: &gmproto.OutgoingRPCMessage_Data{
			RequestID:  requestID,
			BugleRoute: gmproto.BugleRoute_DataEvent,
			MessageTypeData: &gmproto.OutgoingRPCMessage_Data_Type{
				EmptyArr:    &gmproto.EmptyArr{},
				MessageType: params.MessageType,
			},
		},
		Auth: &gmproto.OutgoingRPCMessage_Auth{
			RequestID:        requestID,
			TachyonAuthToken: s.client.AuthData.TachyonAuthToken,
			ConfigVersion:    util.ConfigMessage,
		},
		DestRegistrationIDs: []string{},
	}
	if s.client.AuthData != nil && s.client.AuthData.DestRegID != uuid.Nil {
		message.DestRegistrationIDs = append(message.DestRegistrationIDs, s.client.AuthData.DestRegID.String())
	}
	if params.CustomTTL != 0 {
		message.TTL = params.CustomTTL
	} else if !params.OmitTTL {
		message.TTL = s.client.AuthData.TachyonTTL
	}
	var encryptedData, unencryptedData []byte
	if params.Data != nil {
		var serializedData []byte
		serializedData, err = proto.Marshal(params.Data)
		if err != nil {
			return "", nil, err
		}
		if params.DontEncrypt {
			unencryptedData = serializedData
		} else {
			encryptedData, err = s.client.AuthData.RequestCrypto.Encrypt(serializedData)
			if err != nil {
				return "", nil, err
			}
		}
	}
	message.Data.MessageData, err = proto.Marshal(&gmproto.OutgoingRPCData{
		RequestID:            requestID,
		Action:               params.Action,
		UnencryptedProtoData: unencryptedData,
		EncryptedProtoData:   encryptedData,
		SessionID:            sessionID,
	})
	if err != nil {
		return "", nil, err
	}

	return requestID, message, err
}

func (s *SessionHandler) queueMessageAck(messageID string) {
	s.ackMapLock.Lock()
	defer s.ackMapLock.Unlock()
	if !slices.Contains(s.ackMap, messageID) {
		s.ackMap = append(s.ackMap, messageID)
		s.client.Logger.Trace().Any("message_id", messageID).Msg("Queued ack for message")
	} else {
		s.client.Logger.Trace().Any("message_id", messageID).Msg("Ack for message was already queued")
	}
}

func (s *SessionHandler) startAckInterval() {
	if s.ackTicker != nil {
		return
	}
	ticker := time.NewTicker(5 * time.Second)
	s.ackTicker = ticker
	go func() {
		for range ticker.C {
			s.sendAckRequest()
		}
	}()
}

func (s *SessionHandler) sendAckRequest() {
	s.ackMapLock.Lock()
	dataToAck := s.ackMap
	s.ackMap = nil
	s.ackMapLock.Unlock()
	if len(dataToAck) == 0 {
		return
	}
	ackMessages := make([]*gmproto.AckMessageRequest_Message, len(dataToAck))
	for i, reqID := range dataToAck {
		ackMessages[i] = &gmproto.AckMessageRequest_Message{
			RequestID: reqID,
			Device:    s.client.AuthData.Browser,
		}
	}
	payload := &gmproto.AckMessageRequest{
		AuthData: &gmproto.AuthMessage{
			RequestID:        uuid.NewString(),
			TachyonAuthToken: s.client.AuthData.TachyonAuthToken,
			Network:          s.client.AuthData.AuthNetwork(),
			ConfigVersion:    util.ConfigMessage,
		},
		EmptyArr: &gmproto.EmptyArr{},
		Acks:     ackMessages,
	}
	url := util.AckMessagesURL
	if s.client.AuthData.HasCookies() {
		url = util.AckMessagesURLGoogle
	}
	_, err := typedHTTPResponse[*gmproto.OutgoingRPCResponse](
		s.client.makeProtobufHTTPRequest(url, payload, ContentTypePBLite),
	)
	if err != nil {
		// TODO retry?
		s.client.Logger.Err(err).Strs("message_ids", dataToAck).Msg("Failed to send acks")
	} else {
		s.client.Logger.Trace().Strs("message_ids", dataToAck).Msg("Sent acks")
	}
}

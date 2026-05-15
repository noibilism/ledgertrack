package v2

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	channelmodels "github.com/formancehq/ledger/internal/channels/models"
	channelrepos "github.com/formancehq/ledger/internal/channels/repositories"
	channelservices "github.com/formancehq/ledger/internal/channels/services"
)

type channelFeeConfigServiceForHTTPTests struct {
	configs map[string]*channelmodels.ChannelFeeConfig
	audits  map[string][]channelmodels.ChannelFeeConfigAudit
}

func newChannelFeeConfigServiceForHTTPTests() *channelFeeConfigServiceForHTTPTests {
	return &channelFeeConfigServiceForHTTPTests{
		configs: map[string]*channelmodels.ChannelFeeConfig{},
		audits:  map[string][]channelmodels.ChannelFeeConfigAudit{},
	}
}

func (s *channelFeeConfigServiceForHTTPTests) UpsertConfig(_ context.Context, req channelservices.UpsertChannelFeeConfigRequest) (*channelmodels.ChannelFeeConfig, error) {
	cfg := &channelmodels.ChannelFeeConfig{
		ChannelID:     req.ChannelID,
		Currency:      strings.ToUpper(req.Currency),
		Enabled:       req.Enabled,
		UserFee:       req.UserFee,
		ProcessingFee: req.ProcessingFee,
	}
	s.configs[req.ChannelID] = cfg
	s.audits[req.ChannelID] = append(s.audits[req.ChannelID], channelmodels.ChannelFeeConfigAudit{
		ChannelID: req.ChannelID,
		Actor:     req.Actor,
		Action:    "upsert",
	})
	return cfg, nil
}

func (s *channelFeeConfigServiceForHTTPTests) GetConfig(_ context.Context, channelID string) (*channelmodels.ChannelFeeConfig, error) {
	cfg, ok := s.configs[channelID]
	if !ok {
		return nil, channelservices.ErrChannelFeesNotFound
	}
	return cfg, nil
}

func (s *channelFeeConfigServiceForHTTPTests) ListConfigs(_ context.Context, _ channelrepos.ChannelFeeConfigFilter) ([]channelmodels.ChannelFeeConfig, error) {
	out := make([]channelmodels.ChannelFeeConfig, 0, len(s.configs))
	for _, cfg := range s.configs {
		out = append(out, *cfg)
	}
	return out, nil
}

func (s *channelFeeConfigServiceForHTTPTests) Compute(context.Context, channelservices.ComputeChannelFeesRequest) (*channelservices.ComputedChannelFees, error) {
	return nil, nil
}

func (s *channelFeeConfigServiceForHTTPTests) Record(context.Context, *channelmodels.ChannelFeeRecord) error {
	return nil
}

func (s *channelFeeConfigServiceForHTTPTests) ListAudits(_ context.Context, channelID string, _ int) ([]channelmodels.ChannelFeeConfigAudit, error) {
	return s.audits[channelID], nil
}

func TestChannelFeeConfigCRUDHTTP(t *testing.T) {
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	svc := newChannelFeeConfigServiceForHTTPTests()
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithChannelFeeConfigService(svc))

	body := `{"currency":"USD","enabled":true,"user_fee":{"type":"flat","flat":"1.00"},"processing_fee":{"type":"percentage","percentage":"1.5"},"actor":"admin"}`
	req := httptest.NewRequest(http.MethodPut, "/test/channels/channel-1/fees/config", strings.NewReader(body))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	cfg, ok := api.DecodeSingleResponse[channelmodels.ChannelFeeConfig](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, "channel-1", cfg.ChannelID)
	require.Equal(t, "USD", cfg.Currency)
	require.True(t, cfg.Enabled)

	req = httptest.NewRequest(http.MethodGet, "/test/channels/channel-1/fees/config", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	cfg, ok = api.DecodeSingleResponse[channelmodels.ChannelFeeConfig](t, rec.Body)
	require.True(t, ok)
	require.Equal(t, "channel-1", cfg.ChannelID)
	require.Equal(t, "USD", cfg.Currency)
}

func TestChannelFeeConfigListAndAuditsHTTP(t *testing.T) {
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	svc := newChannelFeeConfigServiceForHTTPTests()
	_, _ = svc.UpsertConfig(context.Background(), channelservices.UpsertChannelFeeConfigRequest{
		ChannelID: "channel-1",
		Currency:  "USD",
		Enabled:   true,
		UserFee: channelmodels.FeeStructure{
			Type: "flat",
		},
		ProcessingFee: channelmodels.FeeStructure{
			Type: "none",
		},
	})
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithChannelFeeConfigService(svc))

	req := httptest.NewRequest(http.MethodGet, "/test/channels/fees/configs", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	type listConfigsResp struct {
		Configs []channelmodels.ChannelFeeConfig `json:"configs"`
	}
	resp, ok := api.DecodeSingleResponse[listConfigsResp](t, rec.Body)
	require.True(t, ok)
	require.NotEmpty(t, resp.Configs)

	req = httptest.NewRequest(http.MethodGet, "/test/channels/channel-1/fees/audits?limit=10", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	type auditsResp struct {
		Audits []channelmodels.ChannelFeeConfigAudit `json:"audits"`
	}
	audits, ok := api.DecodeSingleResponse[auditsResp](t, rec.Body)
	require.True(t, ok)
	require.Len(t, audits.Audits, 1)
}

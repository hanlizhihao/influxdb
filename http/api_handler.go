package http

import (
	"net/http"
	"strings"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/authorizer"
	"github.com/influxdata/influxdb/chronograf/server"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/storage"
	"go.uber.org/zap"
)

// APIHandler is a collection of all the service handlers.
type APIHandler struct {
	BucketHandler        *BucketHandler
	UserHandler          *UserHandler
	OrgHandler           *OrgHandler
	AuthorizationHandler *AuthorizationHandler
	DashboardHandler     *DashboardHandler
	LabelHandler         *LabelHandler
	AssetHandler         *AssetHandler
	ChronografHandler    *ChronografHandler
	ScraperHandler       *ScraperHandler
	SourceHandler        *SourceHandler
	MacroHandler         *MacroHandler
	TaskHandler          *TaskHandler
	TelegrafHandler      *TelegrafHandler
	QueryHandler         *FluxHandler
	ProtoHandler         *ProtoHandler
	WriteHandler         *WriteHandler
	SetupHandler         *SetupHandler
	SessionHandler       *SessionHandler
	SwaggerHandler       http.HandlerFunc
}

// APIBackend is all services and associated parameters required to construct
// an APIHandler.
type APIBackend struct {
	DeveloperMode bool
	Logger        *zap.Logger

	NewBucketService func(*influxdb.Source) (influxdb.BucketService, error)
	NewQueryService  func(*influxdb.Source) (query.ProxyQueryService, error)

	PointsWriter                    storage.PointsWriter
	AuthorizationService            influxdb.AuthorizationService
	BucketService                   influxdb.BucketService
	SessionService                  influxdb.SessionService
	UserService                     influxdb.UserService
	OrganizationService             influxdb.OrganizationService
	UserResourceMappingService      influxdb.UserResourceMappingService
	LabelService                    influxdb.LabelService
	DashboardService                influxdb.DashboardService
	DashboardOperationLogService    influxdb.DashboardOperationLogService
	BucketOperationLogService       influxdb.BucketOperationLogService
	UserOperationLogService         influxdb.UserOperationLogService
	OrganizationOperationLogService influxdb.OrganizationOperationLogService
	SourceService                   influxdb.SourceService
	MacroService                    influxdb.MacroService
	BasicAuthService                influxdb.BasicAuthService
	OnboardingService               influxdb.OnboardingService
	ProxyQueryService               query.ProxyQueryService
	TaskService                     influxdb.TaskService
	TelegrafService                 influxdb.TelegrafConfigStore
	ScraperTargetStoreService       influxdb.ScraperTargetStoreService
	SecretService                   influxdb.SecretService
	LookupService                   influxdb.LookupService
	ChronografService               *server.Service
	ProtoService                    influxdb.ProtoService
	OrgLookupService                authorizer.OrganizationService
}

// NewAPIHandler constructs all api handlers beneath it and returns an APIHandler
func NewAPIHandler(b *APIBackend) *APIHandler {
	h := &APIHandler{}

	sessionBackend := NewSessionBackend(b)
	h.SessionHandler = NewSessionHandler(sessionBackend)
	b.UserResourceMappingService = authorizer.NewURMService(b.OrgLookupService, b.UserResourceMappingService)

	h.BucketHandler = NewBucketHandler(b.UserResourceMappingService, b.LabelService, b.UserService)
	h.BucketHandler.BucketService = authorizer.NewBucketService(b.BucketService)
	h.BucketHandler.OrganizationService = b.OrganizationService
	h.BucketHandler.BucketOperationLogService = b.BucketOperationLogService

	h.LabelHandler = NewLabelHandler()
	h.LabelHandler.LabelService = b.LabelService

	h.OrgHandler = NewOrgHandler(b.UserResourceMappingService, b.LabelService, b.UserService)
	h.OrgHandler.OrganizationService = authorizer.NewOrgService(b.OrganizationService)
	h.OrgHandler.OrganizationOperationLogService = b.OrganizationOperationLogService
	h.OrgHandler.SecretService = b.SecretService

	h.UserHandler = NewUserHandler()
	h.UserHandler.UserService = authorizer.NewUserService(b.UserService)
	h.UserHandler.BasicAuthService = b.BasicAuthService
	h.UserHandler.UserOperationLogService = b.UserOperationLogService

	h.DashboardHandler = NewDashboardHandler(b.UserResourceMappingService, b.LabelService, b.UserService)
	h.DashboardHandler.DashboardService = authorizer.NewDashboardService(b.DashboardService)
	h.DashboardHandler.DashboardOperationLogService = b.DashboardOperationLogService

	h.MacroHandler = NewMacroHandler()
	h.MacroHandler.MacroService = authorizer.NewMacroService(b.MacroService)

	h.AuthorizationHandler = NewAuthorizationHandler(b.UserService)
	h.AuthorizationHandler.OrganizationService = b.OrganizationService
	h.AuthorizationHandler.AuthorizationService = authorizer.NewAuthorizationService(b.AuthorizationService)
	h.AuthorizationHandler.LookupService = b.LookupService
	h.AuthorizationHandler.Logger = b.Logger.With(zap.String("handler", "auth"))

	h.ScraperHandler = NewScraperHandler()
	h.ScraperHandler.ScraperStorageService = authorizer.NewScraperTargetStoreService(b.ScraperTargetStoreService)
	h.ScraperHandler.BucketService = b.BucketService
	h.ScraperHandler.OrganizationService = b.OrganizationService
	h.ScraperHandler.Logger = b.Logger.With(zap.String("handler", "scraper"))

	h.SourceHandler = NewSourceHandler()
	h.SourceHandler.SourceService = authorizer.NewSourceService(b.SourceService)
	h.SourceHandler.NewBucketService = b.NewBucketService
	h.SourceHandler.NewQueryService = b.NewQueryService

	h.SetupHandler = NewSetupHandler()
	h.SetupHandler.OnboardingService = b.OnboardingService

	h.TaskHandler = NewTaskHandler(b.UserResourceMappingService, b.LabelService, b.Logger, b.UserService)
	h.TaskHandler.TaskService = b.TaskService
	h.TaskHandler.AuthorizationService = b.AuthorizationService
	h.TaskHandler.OrganizationService = b.OrganizationService
	h.TaskHandler.UserResourceMappingService = b.UserResourceMappingService

	h.TelegrafHandler = NewTelegrafHandler(
		b.Logger.With(zap.String("handler", "telegraf")),
		b.UserResourceMappingService,
		b.LabelService,
		authorizer.NewTelegrafConfigService(b.TelegrafService, b.UserResourceMappingService),
		b.UserService,
		b.OrganizationService,
	)

	h.WriteHandler = NewWriteHandler(b.PointsWriter)
	h.WriteHandler.OrganizationService = b.OrganizationService
	h.WriteHandler.BucketService = b.BucketService
	h.WriteHandler.Logger = b.Logger.With(zap.String("handler", "write"))

	h.QueryHandler = NewFluxHandler()
	h.QueryHandler.OrganizationService = b.OrganizationService
	h.QueryHandler.Logger = b.Logger.With(zap.String("handler", "query"))
	h.QueryHandler.ProxyQueryService = b.ProxyQueryService

	h.ProtoHandler = NewProtoHandler(NewProtoBackend(b))

	h.ChronografHandler = NewChronografHandler(b.ChronografService)

	h.SwaggerHandler = SwaggerHandler()

	return h
}

var apiLinks = map[string]interface{}{
	// when adding new links, please take care to keep this list alphabetical
	// as this makes it easier to verify values against the swagger document.
	"authorizations": "/api/v2/authorizations",
	"buckets":        "/api/v2/buckets",
	"dashboards":     "/api/v2/dashboards",
	"external": map[string]string{
		"statusFeed": "https://www.influxdata.com/feed/json",
	},
	"labels": "/api/v2/labels",
	"macros": "/api/v2/macros",
	"me":     "/api/v2/me",
	"orgs":   "/api/v2/orgs",
	"protos": "/api/v2/protos",
	"query": map[string]string{
		"self":        "/api/v2/query",
		"ast":         "/api/v2/query/ast",
		"analyze":     "/api/v2/query/analyze",
		"spec":        "/api/v2/query/spec",
		"suggestions": "/api/v2/query/suggestions",
	},
	"setup":    "/api/v2/setup",
	"signin":   "/api/v2/signin",
	"signout":  "/api/v2/signout",
	"sources":  "/api/v2/sources",
	"scrapers": "/api/v2/scrapers",
	"swagger":  "/api/v2/swagger.json",
	"system": map[string]string{
		"metrics": "/metrics",
		"debug":   "/debug/pprof",
		"health":  "/health",
	},
	"tasks":     "/api/v2/tasks",
	"telegrafs": "/api/v2/telegrafs",
	"users":     "/api/v2/users",
	"write":     "/api/v2/write",
}

func (h *APIHandler) serveLinks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := encodeResponse(ctx, w, http.StatusOK, apiLinks); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

// ServeHTTP delegates a request to the appropriate subhandler.
func (h *APIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	setCORSResponseHeaders(w, r)
	if r.Method == "OPTIONS" {
		return
	}

	// Serve the links base links for the API.
	if r.URL.Path == "/api/v2/" || r.URL.Path == "/api/v2" {
		h.serveLinks(w, r)
		return
	}

	if r.URL.Path == "/api/v2/signin" || r.URL.Path == "/api/v2/signout" {
		h.SessionHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/setup") {
		h.SetupHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/write") {
		h.WriteHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/query") {
		h.QueryHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/buckets") {
		h.BucketHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/labels") {
		h.LabelHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/users") {
		h.UserHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/me") {
		h.UserHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/orgs") {
		h.OrgHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/authorizations") {
		h.AuthorizationHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/dashboards") {
		h.DashboardHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/sources") {
		h.SourceHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/scrapers") {
		h.ScraperHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/tasks") {
		h.TaskHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/telegrafs") {
		h.TelegrafHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/macros") {
		h.MacroHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/protos") {
		h.ProtoHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/chronograf/") {
		h.ChronografHandler.ServeHTTP(w, r)
		return
	}

	if r.URL.Path == "/api/v2/swagger.json" {
		h.SwaggerHandler.ServeHTTP(w, r)
		return
	}

	notFoundHandler(w, r)
}

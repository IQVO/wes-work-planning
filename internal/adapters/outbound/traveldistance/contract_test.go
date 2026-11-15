package traveldistance

// Consumer contract tests (R1-R4) against the byte-pinned producer
// spec. testdata/facility-layout.openapi.yaml is a byte copy of
// IQVO/facility-layout develop's apis/openapi.yaml; contracts/pinned.json
// and the pinned-contracts CI job keep it equal to the producer.
//
// R1 the route the client calls is declared; R2 the example fixtures
// satisfy the producer's schemas; R3 the fixtures flow through the REAL
// client call (404/422 map to Known=false with nil error, anything but
// 200/404/422 to ErrUnexpectedStatus, per the client's own doc comment);
// R4 every wire field travelDistanceResponse reads is specified with a
// compatible type.
//
// The spec carries no whole-response example for /distance (per-field
// examples only): the fixtures are DERIVED from its schemas (producer
// example backfill pending — noted in the PR body).

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

// distanceCallSites is the REST surface this adapter consumes: the one
// synchronous call the travel-distance client makes into
// facility-layout (FACILITY_LAYOUT_BASE_URL).
var distanceCallSites = []struct{ method, path string }{
	{http.MethodGet, "/distance"},
}

func loadPinnedSpec(t *testing.T) *openapi3.T {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile("testdata/facility-layout.openapi.yaml")
	if err != nil {
		t.Fatalf("load pinned spec: %v", err)
	}
	return doc
}

func pinnedSchema(t *testing.T, doc *openapi3.T, name string) *openapi3.Schema {
	t.Helper()
	ref := doc.Components.Schemas[name]
	if ref == nil || ref.Value == nil {
		t.Fatalf("schema %s is not in the pinned spec copy", name)
	}
	return ref.Value
}

// R1: the pinned spec declares every (method, path) the client calls,
// and the table names every request the client's code builds, so a new
// call site fails here until it is both declared by the producer and
// pinned.
func TestPinnedSpecCoversEveryCallSite(t *testing.T) {
	src, err := os.ReadFile("client.go")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Count(string(src), "http.NewRequestWithContext"), len(distanceCallSites); got != want {
		t.Errorf("client.go builds %d requests, the call-site table names %d: update the table and the pin", got, want)
	}
	doc := loadPinnedSpec(t)
	for _, c := range distanceCallSites {
		item := doc.Paths.Find(c.path)
		if item == nil {
			t.Errorf("pinned spec has no path %s, the client calls it", c.path)
			continue
		}
		if item.Operations()[c.method] == nil {
			t.Errorf("pinned spec path %s has no %s operation", c.path, c.method)
		}
	}
	// The from/to query parameters the client sends are declared
	// (operation level here; fleet specs may also hoist them to the
	// path-item level, which kin-openapi's Parameters() merges).
	op := doc.Paths.Find("/distance").Operations()[http.MethodGet]
	params := op.Parameters
	if len(params) == 0 {
		t.Fatal("/distance declares no parameters")
	}
	for _, want := range []string{"from", "to"} {
		if params.GetByInAndName("query", want) == nil {
			t.Errorf("/distance does not declare the %s query parameter the client sends", want)
		}
	}
}

// distanceFixtures carries the schema-derived example documents: the
// 200 TravelDistance answer (values from the schema's own per-field
// examples) and problem+json answers shaped by the shared Problem
// schema.
const (
	travelDistanceFixture = `{"metresM":4.4,"estimated":true,"route":[{"aisleId":"WH1-STOR-AMB-A07","bay":"02"},{"aisleId":"WH1-STOR-AMB-A09","bay":"03"}]}`
	notFoundFixture       = `{"type":"https://errors.facility-layout.warehouse-systems.dev/site-not-found","title":"Site not found","status":404,"detail":"site not found","instance":"/sites/NOPE"}`
	unprocessableFixture  = `{"type":"https://errors.facility-layout.warehouse-systems.dev/placement-rule-violated","title":"Location type is not legal in this zone","status":422,"detail":"location type violates a placement rule","instance":"/locations"}`
	internalFixture       = `{"type":"https://errors.facility-layout.warehouse-systems.dev/internal","title":"Internal error","status":500,"detail":"boom","instance":"/distance"}`
)

// R2: every fixture satisfies the pinned schema of the message it
// stands for: the 200 answer is a TravelDistance, the problem answers
// are Problems.
func TestExamplesSatisfyPinnedSpec(t *testing.T) {
	doc := loadPinnedSpec(t)
	for name, tc := range map[string]struct {
		fixture string
		schema  string
	}{
		"travel-distance.answer.json": {travelDistanceFixture, "TravelDistance"},
		"not-found.problem.json":      {notFoundFixture, "Problem"},
		"unprocessable.problem.json":  {unprocessableFixture, "Problem"},
		"internal.problem.json":       {internalFixture, "Problem"},
	} {
		var v any
		if err := json.Unmarshal([]byte(tc.fixture), &v); err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if err := pinnedSchema(t, doc, tc.schema).VisitJSON(v); err != nil {
			t.Errorf("%s does not satisfy the pinned %s schema: %v", name, tc.schema, err)
		}
	}
}

// serveDistance answers the client's one call with a fixture, capturing
// the request the client actually built.
func serveDistance(t *testing.T, status int, body string) (*Client, *capturedRequest) {
	t.Helper()
	captured := &capturedRequest{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured.method, captured.path, captured.query, captured.accept = r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("Accept")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return NewClient(srv.URL, srv.Client()), captured
}

type capturedRequest struct {
	method, path, query, accept string
}

// R3: the fixtures flow through the REAL client call. A 200 answer
// decodes into the Known=true view carrying the wire's metres/estimated
// flag; 404 and 422 are real answers mapped to Known=false with a nil
// error (fail-open per ADR-0017); any other status is
// ErrUnexpectedStatus; the request carries from/to exactly as the spec
// documents them.
func TestClientDecodesSpecExamples(t *testing.T) {
	t.Run("200 answer", decodesSpecAnswerRequest)
	t.Run("GetDistance fail-open", decodesSpecFailOpen)
	t.Run("404 problem", decodesSpecNotFound)
	t.Run("422 problem", decodesSpecUnprocessable)
	t.Run("500 problem", decodesSpecUnexpected)
}

// decodesSpecAnswerRequest: a 200 decodes into the Known=true view and
// the request carries GET /distance with the documented from/to query
// and the Accept header.
func decodesSpecAnswerRequest(t *testing.T) {
	client, captured := serveDistance(t, http.StatusOK, travelDistanceFixture)
	view, err := client.fetch(t.Context(), "WH1-STOR-AMB-A07-01-01-A", "WH1-STOR-AMB-A09-03-01-A")
	if err != nil {
		t.Fatal(err)
	}
	if !view.Known || view.MetresM != 4.4 || !view.Estimated {
		t.Errorf("view = %+v, want Known=true metres=4.4 estimated=true", view)
	}
	if captured.method != http.MethodGet || captured.path != "/distance" {
		t.Errorf("request = %s %s, want GET /distance", captured.method, captured.path)
	}
	if want := "from=WH1-STOR-AMB-A07-01-01-A&to=WH1-STOR-AMB-A09-03-01-A"; captured.query != want {
		t.Errorf("query = %q, want %q", captured.query, want)
	}
	if captured.accept != "application/json" {
		t.Errorf("Accept = %q, want application/json", captured.accept)
	}
}

// decodesSpecFailOpen: the fail-open GetDistance wrapper returns the
// Known=true view on a 200.
func decodesSpecFailOpen(t *testing.T) {
	client, _ := serveDistance(t, http.StatusOK, travelDistanceFixture)
	view, err := client.GetDistance(t.Context(), "WH1-STOR-AMB-A07-01-01-A", "WH1-STOR-AMB-A09-03-01-A")
	if err != nil {
		t.Fatal(err)
	}
	if !view.Known {
		t.Errorf("GetDistance = %+v, want Known=true on a 200", view)
	}
}

// decodesSpecNotFound: a 404 problem+json is a real answer —
// Known=false, nil error.
func decodesSpecNotFound(t *testing.T) {
	client, _ := serveDistance(t, http.StatusNotFound, notFoundFixture)
	view, err := client.fetch(t.Context(), "WH1-NOPE-AMB-A07-01-01-A", "WH1-STOR-AMB-A09-03-01-A")
	if err != nil {
		t.Fatalf("404 must be a real answer (nil error), got %v", err)
	}
	if view.Known {
		t.Error("404 must map to Known=false")
	}
}

// decodesSpecUnprocessable: a 422 problem+json is a real answer —
// Known=false, nil error (fail-open, ADR-0017).
func decodesSpecUnprocessable(t *testing.T) {
	client, _ := serveDistance(t, http.StatusUnprocessableEntity, unprocessableFixture)
	view, err := client.fetch(t.Context(), "WH1-STOR-AMB-A07-01-01-A", "WH1-STOR-AMB-FRZ-A09-03-01-A")
	if err != nil {
		t.Fatalf("422 must be a real answer (nil error), got %v", err)
	}
	if view.Known {
		t.Error("422 must map to Known=false")
	}
}

// decodesSpecUnexpected: any other status is ErrUnexpectedStatus.
func decodesSpecUnexpected(t *testing.T) {
	client, _ := serveDistance(t, http.StatusInternalServerError, internalFixture)
	_, err := client.fetch(t.Context(), "WH1-STOR-AMB-A07-01-01-A", "WH1-STOR-AMB-A09-03-01-A")
	if !errors.Is(err, ErrUnexpectedStatus) {
		t.Errorf("500 = %v, want ErrUnexpectedStatus", err)
	}
}

// R4: every wire field travelDistanceResponse (and its nested
// travelNodeResponse) reads is declared by the pinned TravelDistance /
// TravelNode schemas with a compatible type.
func TestFieldsWeReadAreSpecified(t *testing.T) {
	doc := loadPinnedSpec(t)
	checkDTOFields(t, reflect.TypeOf(travelDistanceResponse{}), pinnedSchema(t, doc, "TravelDistance"), "TravelDistance")
}

// checkDTOFields walks a response DTO against a pinned schema: every
// wire field must be declared with a compatible type, and every field
// the schema does not require must tolerate absence (pointer, slice or
// omitempty).
func checkDTOFields(t *testing.T, dt reflect.Type, sch *openapi3.Schema, path string) {
	t.Helper()
	props, required := flattenSchema(sch)
	for i := range dt.NumField() {
		f := dt.Field(i)
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "" || name == "-" {
			continue // not a wire field
		}
		p := props[name]
		if p == nil || p.Value == nil {
			t.Errorf("%s.%s: json %q is not declared by the pinned schema", path, f.Name, name)
			continue
		}
		checkWireType(t, f.Type, p.Value, path+"."+name)
		if !required[name] && !toleratesAbsence(f) {
			t.Errorf("%s.%s: optional in the pinned schema, so the DTO must tolerate its absence (pointer, slice or omitempty)", path, f.Name)
		}
	}
}

func flattenSchema(sch *openapi3.Schema) (map[string]*openapi3.SchemaRef, map[string]bool) {
	props := map[string]*openapi3.SchemaRef{}
	required := map[string]bool{}
	var walk func(*openapi3.Schema)
	walk = func(s *openapi3.Schema) {
		if s == nil {
			return
		}
		for n, p := range s.Properties {
			props[n] = p
		}
		for _, r := range s.Required {
			required[r] = true
		}
		for _, b := range s.AllOf {
			walk(b.Value)
		}
	}
	walk(sch)
	return props, required
}

func toleratesAbsence(f reflect.StructField) bool {
	_, _, has := strings.Cut(f.Tag.Get("json"), ",omitempty")
	return has || f.Type.Kind() == reflect.Pointer || f.Type.Kind() == reflect.Slice
}

// scalarWireTypes maps the scalar Go kinds a DTO may carry to the
// OpenAPI type that must declare them.
var scalarWireTypes = map[reflect.Kind]string{
	reflect.String: "string", reflect.Bool: "boolean",
	reflect.Int: "integer", reflect.Int8: "integer", reflect.Int16: "integer", reflect.Int32: "integer", reflect.Int64: "integer",
	reflect.Uint: "integer", reflect.Uint8: "integer", reflect.Uint16: "integer", reflect.Uint32: "integer", reflect.Uint64: "integer",
	reflect.Float32: "number", reflect.Float64: "number",
}

func checkWireType(t *testing.T, dt reflect.Type, sch *openapi3.Schema, path string) {
	t.Helper()
	switch dt.Kind() {
	case reflect.Pointer:
		checkWireType(t, dt.Elem(), sch, path)
	case reflect.Slice:
		checkArrayWireType(t, dt.Elem(), sch, path)
	case reflect.Struct:
		checkObjectWireType(t, dt, sch, path)
	default:
		if want, scalar := scalarWireTypes[dt.Kind()]; scalar {
			assertWireType(t, sch, path, want)
		} else {
			t.Errorf("%s: Go kind %s has no wire mapping", path, dt.Kind())
		}
	}
}

func checkArrayWireType(t *testing.T, elem reflect.Type, sch *openapi3.Schema, path string) {
	t.Helper()
	if assertWireType(t, sch, path, "array") && sch.Items != nil && sch.Items.Value != nil {
		checkWireType(t, elem, sch.Items.Value, path+"[]")
	}
}

func checkObjectWireType(t *testing.T, dt reflect.Type, sch *openapi3.Schema, path string) {
	t.Helper()
	if assertWireType(t, sch, path, "object") {
		checkDTOFields(t, dt, sch, path)
	}
}

func assertWireType(t *testing.T, sch *openapi3.Schema, path, want string) bool {
	t.Helper()
	if len(sch.Type.Slice()) > 0 && !sch.Type.Includes(want) {
		t.Errorf("%s: pinned schema type %v, the Go field is %s", path, sch.Type.Slice(), want)
		return false
	}
	return true
}

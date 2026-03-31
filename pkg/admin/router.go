package admin

import (
	"net/http"
	"strings"
)

// Route represents a single HTTP route.
type Route struct {
	Method  string
	Pattern string
	Handler func(http.ResponseWriter, *http.Request, map[string]string)
}

// Router matches HTTP requests to registered routes.
type Router struct {
	routes []Route
	notFound http.HandlerFunc
}

// NewRouter creates a new Router.
func NewRouter() *Router {
	return &Router{
		notFound: func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusNotFound, map[string]interface{}{"ok": false, "error": "not found"})
		},
	}
}

// Handle registers a route with a handler that receives path params.
func (r *Router) Handle(method, pattern string, handler func(http.ResponseWriter, *http.Request, map[string]string)) {
	r.routes = append(r.routes, Route{Method: method, Pattern: pattern, Handler: handler})
}

// ServeHTTP implements http.Handler.
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	path := cleanPath(req.URL.Path)
	for _, route := range r.routes {
		if route.Method != req.Method {
			continue
		}
		params, ok := matchPattern(route.Pattern, path)
		if ok {
			route.Handler(w, req, params)
			return
		}
	}
	r.notFound(w, req)
}

func cleanPath(p string) string {
	if p == "" {
		return "/"
	}
	for len(p) > 1 && p[len(p)-1] == '/' {
		p = p[:len(p)-1]
	}
	return p
}

func matchPattern(pattern, path string) (map[string]string, bool) {
	pp := strings.Split(pattern, "/")
	up := strings.Split(path, "/")
	if len(pp) != len(up) {
		return nil, false
	}
	params := make(map[string]string)
	for i := range pp {
		if strings.HasPrefix(pp[i], ":") {
			params[pp[i][1:]] = up[i]
		} else if pp[i] != up[i] {
			return nil, false
		}
	}
	return params, true
}

package httpd

import (
	"fmt"
	"net/http"
	"testing"
)

func TestNewBalance(t *testing.T) {
	balance := NewBalance()

	remoteMetaData := make(map[string][]string, 0)
	remoteMetaData["test"] = []string{"127.0.0.1:9090"}
	localMeasurement := make(map[string]interface{})
	balance.SetMeasurementMapIndex(remoteMetaData, localMeasurement)

	go func() {
		err := http.ListenAndServe(":9090", &hello{})
		if err != nil {
			fmt.Print("错误")
		}
	}()
	http.ListenAndServe(":8080", &testBalance{
		b: balance,
	})
	//go func(balance *QueryBalance) {
	//}(balance)
}

type hello struct {
}

func (h *hello) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	w.Write([]byte("hello"))
}

type testBalance struct {
	b *QueryBalance
}

func (h *testBalance) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.b.forwardRequest("test", &w, r)
}

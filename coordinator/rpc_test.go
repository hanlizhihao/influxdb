package coordinator

import (
	"net"
	"net/rpc"
	"testing"
)

func TestRpcService_Open(t *testing.T) {
	rpcService := NewRpcService(nil, nil)
	err := rpc.Register(rpcService.queryExecutor)
	if err != nil {
		println(err.Error())
	}
	tcpAddr, err := net.ResolveTCPAddr("tcp", rpcService.rpcConfig.BindAddress)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	listener.Accept()
}

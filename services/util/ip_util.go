package util

import (
	"errors"
	"net"
)

func GetLocalHostIp() (ip string, err error) {
	localhostAddress, err := net.InterfaceAddrs()
	if err != nil {
		return "", errors.New(err.Error())
	}
	for _, address := range localhostAddress {
		if ipNet, ok := address.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				ip := ipNet.IP.String()
				return ip, nil
			}
		}
	}
	return "", errors.New("cannot get localhost ip")
}

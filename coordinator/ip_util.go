package coordinator

import (
	"errors"
	"fmt"
	"net"
)

var ip string

func GetLocalHostIp() (ip string, err error) {
	if &ip != nil {
		return ip, nil
	}
	netInterfaces, err := net.Interfaces()
	if err != nil {
		fmt.Println("net.Interfaces failed, err:", err.Error())
		return "", err
	}

	for i := 0; i < len(netInterfaces); i++ {
		if (netInterfaces[i].Flags & net.FlagUp) != 0 {
			addrs, _ := netInterfaces[i].Addrs()

			for _, address := range addrs {
				if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
					if ipnet.IP.To4() != nil {
						ip = ipnet.IP.String()
						return ip, nil
					}
				}
			}
		}
	}

	return "", errors.New("cannot get localhost ip")
}

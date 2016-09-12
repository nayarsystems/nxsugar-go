package nxsugar

import (
	"net"
	"os/user"
	"strings"
	"time"

	"github.com/kardianos/osext"
	"github.com/miekg/dns"
	nxcli "github.com/nayarsystems/nxgo"
)

var Info struct {
	Version      string
	NxcliVersion string
	WanIp        string
	LanIps       []string
	User         string
	Dir          string
	Started      time.Time
}

func init() {
	Info.NxcliVersion = nxcli.Version.String()
	go getWanIp()
	Info.LanIps = getLanIps()
	Info.User = getExecUser()
	Info.Dir = getExecDir()
	Info.Started = time.Now()
}

func getWanIp() {
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn("myip.opendns.com"), dns.TypeA)
	c := new(dns.Client)
	for i := 0; i < 10; i++ {
		in, _, err := c.Exchange(m, "resolver1.opendns.com:53")
		if err == nil {
			if len(in.Answer) != 0 {
				spl := strings.Split(in.Answer[0].String(), "\t")
				Info.WanIp = spl[len(spl)-1]
				return
			}
		}
		time.Sleep(time.Minute)
	}
}

func getLanIps() []string {
	ips := []string{}
	ifaces, err := net.Interfaces()
	if err != nil {
		return ips
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			ips = append(ips, ip.String())
		}
	}
	return ips
}

func getExecUser() string {
	if u, err := user.Current(); err == nil {
		return u.Username
	}
	return ""
}

func getExecDir() string {
	if d, err := osext.ExecutableFolder(); err == nil {
		return d
	}
	return ""
}

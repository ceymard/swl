package swllib

import (
	"fmt"
	"log"
	"net"
	"os/user"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/rgzr/sshtun"
)

// TunneledURI represents a URI that may be tunneled through SSH
type TunneledURI struct {
	ClientURI   string
	OriginalURI string
	tunnel      *sshtun.SSHTun
}

// IsTunnel returns true if the URI is behind an SSH tunnel
func (u *TunneledURI) IsTunnel() bool {
	return u.tunnel != nil
}

// Close is to be called in a defer statement
func (u *TunneledURI) Close() {
	// log.Print("closed ", u.ClientURI)
	if u.tunnel != nil {
		u.tunnel.Stop()
	}
}

var reSSHTunnel = regexp.MustCompile(`(?P<original_host>[-_$a-zA-Z0-9.]+)(?::(?P<original_port>\d+))?@@(?P<target_host>[-_a-zA-Z0-9.]+)`)

// HandleURI provides a way to set up a local SSH tunnel before opening a connection
// We scan the URI for an SSH tunnel and open a connection to it.
// Note ; the tunnel MUST be closed using Close().
func HandleURI(uri string) (*TunneledURI, error) {

	var (
		err                 error
		host, port, sshhost string
		remote_port         int
		free_local_port     int
		usr                 *user.User
		wg                  sync.WaitGroup
		tunnel              *sshtun.SSHTun
	)

	var matches = reSSHTunnel.FindStringSubmatch(uri)
	if matches != nil {
		host, port, sshhost = matches[1], matches[2], matches[3]
		remote_port, _ = strconv.Atoi(port) // regexp specifies \d, this cannot fail

		if free_local_port, err = getFreePort(); err != nil {
			return nil, fmt.Errorf(`for uri "%s", could not find available port %w`, uri, err)
		}

		if usr, err = user.Current(); err != nil {
			return nil, fmt.Errorf(`for uri "%s", could not get user %w`, uri, err)
		}

		tunnel = sshtun.New(free_local_port, sshhost, remote_port)
		wg.Add(1)
		tunnel.SetRemoteHost(host)
		tunnel.SetUser(usr.Username) // FIXME

		tunnel.SetConnState(func(tun *sshtun.SSHTun, state sshtun.ConnState) {
			if state == sshtun.StateStarting {
				return
			}
			tunnel.SetConnState(nil)
			wg.Done()
		})

		// How does one wait for the start
		go func() {
			err = tunnel.Start()
			if err != nil {
				err = fmt.Errorf(`could not start tunnel %w`, err)
				log.Print(err)
			}
		}()

		wg.Wait()

		var final_host = `127.0.0.1:` + strconv.Itoa(free_local_port)

		return &TunneledURI{
			ClientURI:   strings.Replace(uri, matches[0], final_host, 1),
			OriginalURI: uri,
			tunnel:      tunnel,
		}, nil
	}

	return &TunneledURI{ClientURI: uri, OriginalURI: uri}, nil
}

// copied from github.com/phayes/freeport to avoid too many packages (only one function used, package exposed only two)
// getFreePort asks the kernel for a free open port that is ready to use.
func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

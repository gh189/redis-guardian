package guardian

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gh189/redis-guardian/cluster"
)

type Config struct {
	Nodes     []string
	Self      string
	Replicas  int
	HTTPPort  int
	Token     string
	Managed   bool
	RedisArgs []string
	DataDir   string
	Interval  time.Duration
	Grace     time.Duration

	BackupDSN          string
	BackupCluster      string
	BackupEnabled      bool
	BackupInterval     time.Duration
	BackupRetain       int
	RestoreOnBootstrap bool
}

func DefaultConfig() Config {
	return Config{
		Replicas:           1,
		HTTPPort:           80,
		Managed:            true,
		DataDir:            "/data",
		Interval:           3 * time.Second,
		Grace:              15 * time.Second,
		BackupEnabled:      true,
		BackupInterval:     15 * time.Minute,
		BackupRetain:       24,
		RestoreOnBootstrap: true,
	}
}

func LoadConfig() (Config, error) {
	c := DefaultConfig()
	c.Self = os.Getenv("REDIS_SELF")
	c.Token = os.Getenv("GUARDIAN_TOKEN")
	c.DataDir = envOr("REDIS_DATA_DIR", c.DataDir)
	c.RedisArgs = strings.Fields(os.Getenv("REDIS_EXTRA_ARGS"))
	c.BackupDSN = os.Getenv("BACKUP_PG_DSN")
	c.BackupCluster = os.Getenv("BACKUP_CLUSTER")
	for _, n := range strings.Split(os.Getenv("REDIS_NODES"), ",") {
		if n = strings.TrimSpace(n); n != "" {
			c.Nodes = append(c.Nodes, n)
		}
	}
	var err error
	if c.Replicas, err = envInt("REDIS_REPLICAS", c.Replicas); err != nil {
		return c, err
	}
	if c.HTTPPort, err = envInt("GUARDIAN_PORT", c.HTTPPort); err != nil {
		return c, err
	}
	if c.Managed, err = envBool("REDIS_MANAGED", c.Managed); err != nil {
		return c, err
	}
	if c.BackupEnabled, err = envBool("BACKUP_ENABLED", c.BackupEnabled); err != nil {
		return c, err
	}
	if c.RestoreOnBootstrap, err = envBool("RESTORE_ON_BOOTSTRAP", c.RestoreOnBootstrap); err != nil {
		return c, err
	}
	if c.BackupInterval, err = envDuration("BACKUP_INTERVAL", c.BackupInterval); err != nil {
		return c, err
	}
	if c.BackupRetain, err = envInt("BACKUP_RETAIN", c.BackupRetain); err != nil {
		return c, err
	}
	if c.BackupRetain <= 0 {
		return c, fmt.Errorf("BACKUP_RETAIN must be greater than 0")
	}
	if c.Interval, err = envDuration("GUARDIAN_INTERVAL", c.Interval); err != nil {
		return c, err
	}
	if c.Grace, err = envDuration("GUARDIAN_GRACE", c.Grace); err != nil {
		return c, err
	}
	return c, c.validate()
}

func (c *Config) validate() error {
	if len(c.Nodes) == 0 {
		return fmt.Errorf("no Redis nodes configured (REDIS_NODES: comma separated host:port list)")
	}
	for _, n := range c.Nodes {
		if _, _, err := splitHostPort(n); err != nil {
			return err
		}
	}
	c.Nodes = slices.Clone(c.Nodes)
	sort.Strings(c.Nodes)
	for i := 1; i < len(c.Nodes); i++ {
		if c.Nodes[i] == c.Nodes[i-1] {
			return fmt.Errorf("node %s is listed twice", c.Nodes[i])
		}
	}
	if c.Replicas < 0 || len(c.Nodes) < c.Replicas+1 {
		return fmt.Errorf("%d replicas per master do not fit %d nodes", c.Replicas, len(c.Nodes))
	}
	def := DefaultConfig()
	if c.HTTPPort == 0 {
		c.HTTPPort = def.HTTPPort
	}
	if c.DataDir == "" {
		c.DataDir = def.DataDir
	}
	dir, err := filepath.Abs(c.DataDir)
	if err != nil {
		return fmt.Errorf("REDIS_DATA_DIR %q: %w", c.DataDir, err)
	}
	c.DataDir = dir
	if c.Interval <= 0 {
		c.Interval = def.Interval
	}
	if c.Grace <= 0 {
		c.Grace = def.Grace
	}
	if c.BackupInterval <= 0 {
		c.BackupInterval = def.BackupInterval
	}
	if c.BackupRetain <= 0 {
		c.BackupRetain = def.BackupRetain
	}
	if c.BackupDSN != "" {
		if c.BackupCluster == "" {
			return fmt.Errorf("BACKUP_CLUSTER is required when BACKUP_PG_DSN is set")
		}
		if !c.Managed {
			return fmt.Errorf("backups need REDIS_MANAGED=true: the guardian reads the RDB files and starts redis-server itself")
		}
	}
	if c.Self == "" {
		c.Self = detectSelf(c.Nodes)
	}
	if !slices.Contains(c.Nodes, c.Self) {
		return fmt.Errorf("cannot tell which of the configured nodes is this one (got %q); set Self / REDIS_SELF", c.Self)
	}
	return nil
}

func (c *Config) masters() int { return len(c.Nodes) / (c.Replicas + 1) }

func (c *Config) weight(name string) int {
	if len(c.Nodes)%2 == 0 && name == c.Nodes[0] {
		return 2
	}
	return 1
}

func (c *Config) quorum() int {
	total := 0
	for _, n := range c.Nodes {
		total += c.weight(n)
	}
	return total/2 + 1
}

func (c *Config) redisPort() int {
	_, port, _ := splitHostPort(c.Self)
	return port
}

func (c *Config) peerURL(name string) string {
	host, _, _ := splitHostPort(name)
	return "http://" + net.JoinHostPort(host, strconv.Itoa(c.HTTPPort))
}

func (c *Config) nameByAddr(n cluster.Node) string {
	for _, name := range c.Nodes {
		host, port, _ := splitHostPort(name)
		if name == n.Addr || (host == n.Hostname && strings.HasSuffix(n.Addr, ":"+strconv.Itoa(port))) {
			return name
		}
	}
	return ""
}

func splitHostPort(name string) (string, int, error) {
	host, p, err := net.SplitHostPort(name)
	if err != nil {
		return "", 0, fmt.Errorf("invalid node %q: %w", name, err)
	}
	port, err := strconv.Atoi(p)
	if err != nil || host == "" {
		return "", 0, fmt.Errorf("invalid node %q", name)
	}
	return host, port, nil
}

func detectSelf(nodes []string) string {
	hostname, _ := os.Hostname()
	for _, n := range nodes {
		host, _, _ := splitHostPort(n)
		if hostname != "" && (host == hostname || strings.HasPrefix(host, hostname+".")) {
			return n
		}
	}
	local := map[string]bool{}
	addrs, _ := net.InterfaceAddrs()
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			local[ipnet.IP.String()] = true
		}
	}
	for _, n := range nodes {
		host, _, _ := splitHostPort(n)
		ips, _ := net.LookupHost(host)
		for _, ip := range append(ips, host) {
			if local[ip] {
				return n
			}
		}
	}
	return ""
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func envBool(key string, def bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s: %w", key, err)
	}
	return b, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s: invalid duration %q", key, v)
	}
	return d, nil
}

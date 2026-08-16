package gafkalo

import internalrbac "github.com/kmetaxas/gafkalo/internal/rbac"

const (
	CTX_KAFKA   = internalrbac.CTX_KAFKA
	CTX_SR      = internalrbac.CTX_SR
	CTX_KSQL    = internalrbac.CTX_KSQL
	CTX_CONNECT = internalrbac.CTX_CONNECT
)

type MDSAdmin = internalrbac.MDSAdmin
type MDSRolebindings = internalrbac.MDSRolebindings
type MDSResourcePattern = internalrbac.MDSResourcePattern
type MDSContext = internalrbac.MDSContext

func NewMDSAdmin(config MDSConfig) *MDSAdmin {
	return internalrbac.NewMDSAdmin(config)
}

package admins

import (
	"github.com/pcristin/monad-faucet/internal/core"
)

// NewAdminHandler creates a new AdminHandler instance
func NewAdminHandler(handler core.HandlerInterface) *AdminHandler {
	return &AdminHandler{
		Handler: handler,
	}
}

package bridge

import (
	"math/big"
)

//
// --- Utility Functions ---
//

func formatMonAmount(amount *big.Int) string {
	f := new(big.Float).SetInt(amount)
	f = new(big.Float).Quo(f, new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))
	return f.Text('f', 6)
}

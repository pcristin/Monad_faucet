// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";

/**
 * @title MonadFaucetDepositContract
 * @dev A contract that accepts deposits in ETH, USDT, and USDC.
 *      It logs all deposit details via events (с дополнительным metadata) and supports parameterized refunds.
 *      No mapping is used to store deposit data on-chain.
 */
contract MonadFaucetDepositContract is Ownable, Pausable, ReentrancyGuard {
    using SafeERC20 for IERC20;

    // Enumeration to represent the type of currency deposited.
    enum CurrencyType { ETH, USDT, USDC }

    // Unique deposit identifier counter.
    uint256 public depositCount;

    // Minimum deposit amounts for each currency.
    uint256 public minEthDeposit = 0.0004 ether;
    uint256 public minUsdtDeposit = 0.25 * 10**6; // For USDT with 6 decimals.
    uint256 public minUsdcDeposit = 0.25 * 10**6; // For USDC with 6 decimals.

    // Hardcoded ERC20 token interfaces for USDT and USDC on Arbitrum.
    IERC20 public usdt = IERC20(0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9);
    IERC20 public usdc = IERC20(0xaf88d065e77c8cC2239327C5EDb3A432268e5831);

    // Events for logging deposits and refunds.
    event DepositEvent(
        address indexed depositor,
        uint256 amount,
        uint256 depositId,
        CurrencyType currency,
        string metadata
    );
    event RefundEvent(address indexed depositor, uint256 amount, uint256 depositId, CurrencyType currency);

    /**
     * @dev Constructor. USDT and USDC addresses are hardcoded.
     */
    constructor() Ownable(_msgSender()) {}

    // ==================== Deposit Functions ====================

    /**
     * @dev Accepts ETH deposits with metadata. Checks the minimum deposit amount and emits a DepositEvent.
     * @param metadata A string up to 32 characters.
     */
    function depositETH(string calldata metadata) external payable whenNotPaused nonReentrant {
        require(msg.value >= minEthDeposit, "Deposit amount too low for ETH");
        require(bytes(metadata).length <= 32, "Metadata must be at most 32 characters");
        depositCount++;
        emit DepositEvent(msg.sender, msg.value, depositCount, CurrencyType.ETH, metadata);
    }

    /**
     * @dev Accepts USDT deposits using transferFrom with metadata. The sender must approve the contract beforehand.
     * @param amount The amount of USDT to deposit (in 6 decimals, e.g. 1 * 10^6).
     * @param metadata A string up to 32 characters.
     */
    function depositUSDT(uint256 amount, string calldata metadata) external whenNotPaused nonReentrant {
        require(amount >= minUsdtDeposit, "Deposit amount too low for USDT");
        require(bytes(metadata).length <= 32, "Metadata must be at most 32 characters");
        usdt.safeTransferFrom(msg.sender, address(this), amount);
        depositCount++;
        emit DepositEvent(msg.sender, amount, depositCount, CurrencyType.USDT, metadata);
    }

    /**
     * @dev Accepts USDC deposits using transferFrom with metadata. The sender must approve the contract beforehand.
     * @param amount The amount of USDC to deposit (in 6 decimals, e.g. 1 * 10^6).
     * @param metadata A string up to 32 characters.
     */
    function depositUSDC(uint256 amount, string calldata metadata) external whenNotPaused nonReentrant {
        require(amount >= minUsdcDeposit, "Deposit amount too low for USDC");
        require(bytes(metadata).length <= 32, "Metadata must be at most 32 characters");
        usdc.safeTransferFrom(msg.sender, address(this), amount);
        depositCount++;
        emit DepositEvent(msg.sender, amount, depositCount, CurrencyType.USDC, metadata);
    }

    // ==================== Withdrawal Functions ====================

    /**
     * @dev Withdraw a specified amount of ETH from the contract to the provided address.
     * @param amount The amount of ETH (in wei) to withdraw.
     * @param to The address to receive the withdrawn ETH.
     */
    function withdrawETH(uint256 amount, address to) external onlyOwner nonReentrant {
        require(to != address(0), "Invalid address");
        require(amount <= address(this).balance, "Insufficient ETH balance");
        (bool success, ) = payable(to).call{value: amount}("");
        require(success, "ETH transfer failed");
    }

    /**
     * @dev Withdraw a specified amount of USDT from the contract to the provided address.
     * @param amount The amount of USDT (in token units, considering decimals) to withdraw.
     * @param to The address to receive the withdrawn USDT.
     */
    function withdrawUSDT(uint256 amount, address to) external onlyOwner nonReentrant {
        require(to != address(0), "Invalid address");
        uint256 balance = usdt.balanceOf(address(this));
        require(amount <= balance, "Insufficient USDT balance");
        usdt.safeTransfer(to, amount);
    }

    /**
     * @dev Withdraw a specified amount of USDC from the contract to the provided address.
     * @param amount The amount of USDC (in token units, considering decimals) to withdraw.
     * @param to The address to receive the withdrawn USDC.
     */
    function withdrawUSDC(uint256 amount, address to) external onlyOwner nonReentrant {
        require(to != address(0), "Invalid address");
        uint256 balance = usdc.balanceOf(address(this));
        require(amount <= balance, "Insufficient USDC balance");
        usdc.safeTransfer(to, amount);
    }

    // ==================== Parameterized Refund Function ====================

    /**
     * @dev Refunds a deposit by providing deposit details. This function does not check for prior refunds.
     *      It is the owner's responsibility to ensure a deposit is refunded only once.
     * @param depositId The unique identifier of the deposit.
     * @param depositor The address of the original depositor.
     * @param amount The amount to refund.
     * @param currency The currency type of the deposit.
     */
    function refundDeposit(
        uint256 depositId,
        address depositor,
        uint256 amount,
        CurrencyType currency
    ) external onlyOwner nonReentrant {
        if (currency == CurrencyType.ETH) {
            (bool success, ) = payable(depositor).call{value: amount}("");
            require(success, "ETH refund failed");
        } else if (currency == CurrencyType.USDT) {
            usdt.safeTransfer(depositor, amount);
        } else if (currency == CurrencyType.USDC) {
            usdc.safeTransfer(depositor, amount);
        }
        emit RefundEvent(depositor, amount, depositId, currency);
    }

    // ==================== Administrative Functions ====================

    /**
     * @dev Allows the owner to update the minimum ETH deposit.
     * @param _minEth The new minimum deposit value in wei.
     */
    function setMinEthDeposit(uint256 _minEth) external onlyOwner {
        minEthDeposit = _minEth;
    }

    /**
     * @dev Allows the owner to update the minimum USDT deposit.
     * @param _minUsdt The new minimum deposit value for USDT.
     */
    function setMinUsdtDeposit(uint256 _minUsdt) external onlyOwner {
        minUsdtDeposit = _minUsdt;
    }

    /**
     * @dev Allows the owner to update the minimum USDC deposit.
     * @param _minUsdc The new minimum deposit value for USDC.
     */
    function setMinUsdcDeposit(uint256 _minUsdc) external onlyOwner {
        minUsdcDeposit = _minUsdc;
    }

    /**
    * @dev Allows the owner to set the deposit count.
    * @param newDepositCount The new value for depositCount.
    */
    function setDepositCount(uint256 newDepositCount) external onlyOwner {
        depositCount = newDepositCount;
    }

    // ==================== Pause/Resume Functionality ====================

    /**
     * @dev Pauses deposit functions. Only callable by the owner.
     */
    function pauseDeposits() external onlyOwner {
        _pause();
    }

    /**
     * @dev Resumes deposit functions. Only callable by the owner.
     */
    function resumeDeposits() external onlyOwner {
        _unpause();
    }
}

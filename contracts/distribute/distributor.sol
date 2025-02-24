/// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/utils/Address.sol";

contract FundDistributor is Ownable, ReentrancyGuard {
    using Address for address payable;

    /// @notice Minimum deposit required for receiving ETH.
    uint256 public minDeposit;

    /// @notice Emitted when ETH is deposited.
    event Deposit(address indexed sender, uint256 amount);

    /// @notice Emitted when a distribution transfer is made.
    event Distribution(address indexed recipient, uint256 amount, uint256 id);

    /// @notice Struct representing a single transfer operation.
    struct TransferData {
        address payable recipient;
        uint256 amount;
        uint256 id;
    }

    /// @notice Constructor sets the initial minimum deposit and initializes the Ownable contract with msg.sender.
    constructor() Ownable(msg.sender) {
        minDeposit = 0.1 ether;
    }

    /// @notice Accepts ETH deposits, ensuring the deposit meets the minimum requirement.
    receive() external payable {
        require(msg.value >= minDeposit, "Deposit amount is below minimum required");
        emit Deposit(msg.sender, msg.value);
    }

    /// @notice Distributes funds to multiple recipients atomically.
    /// @dev Only callable by the owner. Reverts the entire transaction if any transfer fails.
    /// @param transfers An array of TransferData structs containing recipient, amount, and id.
    function distributeFunds(TransferData[] calldata transfers) external onlyOwner nonReentrant {
        uint256 totalAmount = 0;
        // Validate each transfer and accumulate the total amount required.
        for (uint256 i = 0; i < transfers.length; i++) {
            require(transfers[i].recipient != address(0), "Invalid recipient address");
            require(transfers[i].amount > 0, "Transfer amount must be greater than zero");
            totalAmount += transfers[i].amount;
        }
        require(address(this).balance >= totalAmount, "Insufficient contract balance");

        // Execute each transfer using a safe call.
        for (uint256 i = 0; i < transfers.length; i++) {
            (bool sent, ) = transfers[i].recipient.call{value: transfers[i].amount}("");
            require(sent, "Failed to send Ether");
            emit Distribution(transfers[i].recipient, transfers[i].amount, transfers[i].id);
        }
    }

    /// @notice Allows the owner to withdraw all ETH from the contract.
    function withdrawAll() external onlyOwner nonReentrant {
        uint256 balance = address(this).balance;
        require(balance > 0, "No funds to withdraw");
        (bool sent, ) = payable(owner()).call{value: balance}("");
        require(sent, "Withdrawal failed");
    }

    /// @notice Updates the minimum deposit required.
    /// @param newMinDeposit The new minimum deposit amount in wei.
    function setMinDeposit(uint256 newMinDeposit) external onlyOwner {
        minDeposit = newMinDeposit;
    }
}

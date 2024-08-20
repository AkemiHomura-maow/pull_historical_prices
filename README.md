# To configure

```
pipx inject eth-brownie pandas
pipx inject eth-brownie eth-abi
brownie pm install OpenZeppelin/openzeppelin-contracts@4.9.0
```


# To run

```
brownie run scripts/main.py --network=base-main
brownie run scripts/main.py --network=optimism-main
```
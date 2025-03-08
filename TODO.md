AttributeDict({'type': 0, 'chainId': 1, 'nonce': 7, 'gasPrice': 12000000000, 'gas': 1000000, 'to': '0xc0a47dFe034B400B47bDaD5FecDa2621de6c4d95', 'value': 0, 'input': HexBytes('0x1648f38e000000000000000000000000b8c77482e45f1f44de1745f52c74426c631bdd52'), 'r': HexBytes('0xad93380862223caa0ac7208d98f194d84bfcefb8fd14e58321cf85ffe7dfd279'), 's': HexBytes('0x3cc0f0d9febcf6329dc8a4272f4b441be75802c3528f27e1ee1dfbffa27540ca'), 'v': 37, 'hash': HexBytes('0xb256d3ed140c7ce36a1434077e36105cd8bac8229e42cf169cd019a66b42d71c'), 'blockHash': HexBytes('0xc147ea2fc764022bf6808ca8a388bf960e9002939ec29bb18ee2903561283ae0'), 'blockNumber': 6627974, 'transactionIndex': 61, 'from': '0xD1C24f50d05946B3FABeFBAe3cd0A7e9938C63F2'})
With arguments: {'token': '0xB8c77482e45F1F44dE1745F52C74426C631bDD52'}
Token Name: BNB
Token Symbol: BNB
Token BNB UniExchange_address: 0x255e60c9d597dCAA66006A904eD36424F7B26286
b'Uniswap V1\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
b'UNI-V1\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
Token Adress: 0xB8c77482e45F1F44dE1745F52C74426C631bDD52
Is this the same ? True

Analyzer les Liquidite avec le proxy en Write !

On ne peut pas directement, il faut recouper le token renvoye par le SM (celui qui represente les liquidite) et refaire le calcul nous meme
+ Le Reth a prune les state donc on a pas les receipt ....
+ Script qui trace l'evolution des tokens ERC-20 d'une address overtime (Long)


-----------------------------------------------------
+ le block avec le process_receipt est super important, on pars de lui pour analyzer tout l'exchange et savoir la quantite de token UNI-V1 issued
  Pour map les liquidites 
-> Essayer d'avoir les address unique
-> check les remove liquidite et tracer sur un graph add/rem et en deduire un volume

- chiant de devoir utiliser ETHERSCAN
- l'exchange BNB est un peu naze, tres peu de liquidite

-----------------------------------------------------
+ additionnal ETHERSCAN API KEY
+ Function de crawling des block pour les interactions avec le smartcontract mais hit 429/402 apres quelques blocks

-> fonction de plot AI generated, it's a mess we need to clean that
-> il y a des subtilite avec la qty d'ETH dans le smartcontract et la qty d'ETH de la pool, il faut faire un peu d'analyze pour avoir tout clean
-> regarde la derniere cell et re-decompose, c'est IA generated il faut s'approprier ce code
==> il faut compute les liquidites/les addresses
-----------------------------------------------------

Ultra improved: Started V2
ON check que tout est ok niveau graph comme la V1
Si oui -> Objectif V3 car super important et plus complique (NFT ?)

Idea:
  - Graph database pour chase les liquidite entre address (ceux qui revendent leurs LP token) pour eviter les share negatives/plus grande que 100 dans le graph des shares
  - Focus analyze des pools:
    - Top 20/50/100 des pools avec le plus de liquidite, de volumes
    - Les fees ?
    - Les Swaps ?


Improve:
- Function de query des symbol/ticker avec du cache
- Function pour list les events par factory contract
- Function pour retrieve les contract de creation d'exchange (V1)/ Pair (V2)
- Function pour list les token pair pour filter
- retrieve les full data (trop long is on met toutes les paires)
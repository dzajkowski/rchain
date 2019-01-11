"""Tests for the testing code itself."""

import random

from .common import (
    KeyPair,
)
from .rnode import (
    extract_block_hash_from_propose_output,
    extract_block_count_from_show_blocks,
    parse_show_blocks_key_value_line,
    parse_show_blocks_output,
)
from .conftest import (
    make_wallets_file_lines,
)


def test_blocks_count_from_show_blocks() -> None:
    show_blocks_output = '''
------------- block 0 ---------------
blockHash: "630c5372c67cc5400a9eb11459bb240226273a693bbb018df829a3119b26bbf0"
blockSize: "99746"
blockNumber: 0
version: 1
deployCount: 10
tupleSpaceHash: "f2fdac324a5fa86f58d3e8162ad5108d9bc75773311d32bb9bc36b74c632793a"
timestamp: 1
faultTolerance: 1.0
mainParentHash: ""
sender: ""

-----------------------------------------------------


count: 123

'''
    assert extract_block_count_from_show_blocks(show_blocks_output) == 123


def test_parse_show_blocks_key_value_line() -> None:
    assert parse_show_blocks_key_value_line('''blockHash: "cf42c994ff30189c35cbd007719c6bdb361b28c70ae88889a6e54b5431b8f7eb"''') == ('blockHash', '"cf42c994ff30189c35cbd007719c6bdb361b28c70ae88889a6e54b5431b8f7eb"')
    assert parse_show_blocks_key_value_line('''blockSize: "111761"''') == ('blockSize', '"111761"')
    assert parse_show_blocks_key_value_line('''blockNumber: 0''') == ('blockNumber', '0')
    assert parse_show_blocks_key_value_line('''version: 1''') == ('version', '1')
    assert parse_show_blocks_key_value_line('''deployCount: 10''') == ('deployCount', '10')
    assert parse_show_blocks_key_value_line('''tupleSpaceHash: "dcd6e349d5b4ca45a11811808ad7757bdfb856b093e02c7e4a2930817f179cdb"''') == ('tupleSpaceHash', '"dcd6e349d5b4ca45a11811808ad7757bdfb856b093e02c7e4a2930817f179cdb"')
    assert parse_show_blocks_key_value_line('''timestamp: 1''') == ('timestamp', '1')
    assert parse_show_blocks_key_value_line('''faultTolerance: -0.6666667''') == ('faultTolerance', '-0.6666667')
    assert parse_show_blocks_key_value_line('''mainParentHash: ""''') == ('mainParentHash', '""')
    assert parse_show_blocks_key_value_line('''sender: ""''') == ('sender', '""')


def test_parse_show_blocks_output() -> None:
    input = '''
------------- block 0 ---------------
blockHash: "cf42c994ff30189c35cbd007719c6bdb361b28c70ae88889a6e54b5431b8f7eb"
blockSize: "111761"
blockNumber: 0
version: 1
deployCount: 10
tupleSpaceHash: "dcd6e349d5b4ca45a11811808ad7757bdfb856b093e02c7e4a2930817f179cdb"
timestamp: 1
faultTolerance: -0.6666667
mainParentHash: ""
sender: ""

-----------------------------------------------------


count: 1

'''

    output = parse_show_blocks_output(input)
    assert len(output) == 1
    block = output[0]
    assert block['blockHash'] == '"cf42c994ff30189c35cbd007719c6bdb361b28c70ae88889a6e54b5431b8f7eb"'
    assert block['blockSize'] == '"111761"'
    assert block['blockNumber'] == '0'
    assert block['version'] == '1'
    assert block['deployCount'] == '10'
    assert block['tupleSpaceHash'] == '"dcd6e349d5b4ca45a11811808ad7757bdfb856b093e02c7e4a2930817f179cdb"'
    assert block['timestamp'] == '1'
    assert block['faultTolerance'] == '-0.6666667'
    assert block['mainParentHash'] == '""'
    assert block['sender'] == '""'


def test_extract_block_hash_from_propose_output() -> None:
    response = "Response: Success! Block a91208047c... created and added.\n"
    assert extract_block_hash_from_propose_output(response) == "a91208047c"


def test_make_wallets_file_lines() -> None:
    random_generator = random.Random(1547120283)
    validator_keys = [
        KeyPair(private_key='80366db5fbb8dad7946f27037422715e4176dda41d582224db87b6c3b783d709', public_key='1cd8bf79a2c1bd0afa160f6cdfeb8597257e48135c9bf5e4823f2875a1492c97'),
        KeyPair(private_key='120d42175739387af0264921bb117e4c4c05fbe2ce5410031e8b158c6e414bb5', public_key='02ab69930f74b931209df3ce54e3993674ab3e7c98f715608a5e74048b332821'),
        KeyPair(private_key='1f52d0bce0a92f5c79f2a88aae6d391ddf853e2eb8e688c5aa68002205f92dad', public_key='043c56051a613623cd024976427c073fe9c198ac2b98315a4baff9d333fbb42e'),
    ]

    output = make_wallets_file_lines(random_generator, validator_keys)

    assert output == [
        '0x1cd8bf79a2c1bd0afa160f6cdfeb8597257e48135c9bf5e4823f2875a1492c97,40,0',
        '0x02ab69930f74b931209df3ce54e3993674ab3e7c98f715608a5e74048b332821,45,0',
        '0x043c56051a613623cd024976427c073fe9c198ac2b98315a4baff9d333fbb42e,26,0',
    ]

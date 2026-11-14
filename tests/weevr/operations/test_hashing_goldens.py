"""Frozen hash key output, locked across Spark versions.

Surrogate keys and change-detection hashes are persisted in users' tables, so a
Spark upgrade that changes any of them is a silent data break: every key differs
and every row looks changed. Key columns are stringified with ``cast("string")``
before hashing, and that rendering (doubles, decimals, timestamps, binary) is
where Spark versions can differ.

The goldens below were captured on Spark 3.5.5 and confirmed unchanged on Spark
4.1.1. They are frozen: never regenerate them to make this test pass. A mismatch
means a data break that has to be designed for, not a stale fixture. The one
planned change is the v2 hash-separator change, which regenerates them
deliberately.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from weevr.model.keys import ChangeDetectionConfig, KeyConfig
from weevr.operations.hashing import compute_keys

ALGORITHMS = ["xxhash64", "sha1", "sha256", "sha384", "sha512", "md5", "crc32", "murmur3"]

# One row per value class; SQL casts keep -0.0, NaN and timestamps exact (session TZ is UTC).
FIXTURE_SQL = """
SELECT * FROM VALUES
  (1, 1, 9223372036854775807, CAST('-0.0' AS DOUBLE), CAST('12345.6789' AS DECIMAL(38,10)),
   DATE'2024-02-29', TIMESTAMP'2024-02-29 23:59:59.123456', true, 'héllo|world', X'00FF'),
  (2, -1, -9223372036854775808, CAST('NaN' AS DOUBLE), CAST('-0.0000000001' AS DECIMAL(38,10)),
   DATE'1970-01-01', TIMESTAMP'1970-01-01 00:00:00', false, '', X''),
  (3, 2147483647, 0, 1.0E300D, CAST('0' AS DECIMAL(38,10)),
   DATE'9999-12-31', TIMESTAMP'2000-01-01 12:00:00.1', true, ' padded ', X'DEADBEEF'),
  (4, -2147483648, 42, 1.0E-300D, CAST('99999999999999999999999999.9999999999' AS DECIMAL(38,10)),
   DATE'2000-01-01', TIMESTAMP'2038-01-19 03:14:08', false, '__NULL__', X'00'),
  (5, 0, 1, 1.0E7D, CAST('1.5' AS DECIMAL(38,10)),
   DATE'2024-01-01', TIMESTAMP'2024-06-30 23:59:59.999999', true, 'a', X'7F'),
  (6, 7, 7, 1234567.0D, CAST('7' AS DECIMAL(38,10)),
   DATE'2024-01-01', TIMESTAMP'2024-01-01 00:00:00.000001', true, 'b', X'01'),
  (7, 8, 8, 0.1D, CAST('0.1' AS DECIMAL(38,10)),
   DATE'2024-01-01', TIMESTAMP'2024-01-01 00:00:00', true, 'c', X'02'),
  (8, NULL, NULL, CAST(NULL AS DOUBLE), CAST(NULL AS DECIMAL(38,10)),
   CAST(NULL AS DATE), CAST(NULL AS TIMESTAMP), CAST(NULL AS BOOLEAN), CAST(NULL AS STRING),
   CAST(NULL AS BINARY))
  AS t(id, i, l, d, dec, dt, ts, b, s, bin)
"""

VALUE_COLUMNS = ["i", "l", "d", "dec", "dt", "ts", "b", "s", "bin"]


def compute_hashes(spark: SparkSession) -> dict[str, list[str | int]]:
    """Every algorithm over all columns, plus sha256 of each column alone, by row id."""
    df = spark.sql(FIXTURE_SQL)
    out: dict[str, list[str | int]] = {}
    for algorithm in ALGORITHMS:
        keys = KeyConfig(
            change_detection=ChangeDetectionConfig(
                name="h", columns=VALUE_COLUMNS, algorithm=algorithm
            )
        )
        rows = compute_keys(df, keys).orderBy("id").select("h").collect()
        out[algorithm] = [r["h"] for r in rows]
    keys = KeyConfig(
        change_detection=ChangeDetectionConfig(
            name="h", columns=VALUE_COLUMNS, algorithm="xxhash64", output="string"
        )
    )
    out["xxhash64/string"] = [r["h"] for r in compute_keys(df, keys).orderBy("id").collect()]
    for column in VALUE_COLUMNS:
        keys = KeyConfig(
            change_detection=ChangeDetectionConfig(name="h", columns=[column], algorithm="sha256")
        )
        out[f"sha256/{column}"] = [
            r["h"] for r in compute_keys(df, keys).orderBy("id").select("h").collect()
        ]
    return out


GOLDENS: dict[str, list[str | int]] = {
    "xxhash64": [
        -942242207906623067,
        -8711574985793415895,
        5965191501987965608,
        2377208065479243432,
        4598369744765628714,
        -3378535398816028078,
        -1144582246501955171,
        3006146523491832284,
    ],
    "sha1": [
        "156f8964c5bfcccb51ef9a55959330af8a119ad0",
        "5db004a851b511225df7f0030fcec5f2a34d23c2",
        "98bf23454bb141df704665248e23d1a3fc092c9a",
        "0e0e41d667e70f2cde8dddcc8bd3e61accc0e9d2",
        "e3d2b6b3ff95d9c33dbc4ce099d25da312e1c917",
        "623eb652b84fd67d8e6a0da568ee99bb6a34aff4",
        "fc302371bd2166258a4a9705f212d404eaae9fe4",
        "6d05b7eb7f0cf215829639c8ba207fab9fc15ac8",
    ],
    "sha256": [
        "27ac9b6ae468509c81a44c2fabe02f8b9b86e7e6a4e88295548c8c61961111ff",
        "c5f98efb0708b2088c48a2e47801c3c10967582ca9e044087fb6fc5b8e81fe31",
        "db575200bc784095c491d80d84cf44ea7ac1e00605ab784fd7e7bb85dd2347e0",
        "286f941889b81e24b918f0c27f630e7dd801a1f228d232f14df1077d202476e4",
        "ef0f4099b9d5b2ede97c3f576a88b5225c99f50d4c87d4f4737a4c80ec34b748",
        "63dd02d946f0129ec39d06b0827569645d36a6c47998d07923dc93c987c4b184",
        "c6e036af27f6a85f9588ef62a16fe704a38affe8c2873fc3326cdd4a5c4aaf71",
        "e168723aa8b049dc580c6c2689850e8e077857263c826611d1c5c79874ff3a01",
    ],
    "sha384": [
        "0a680b6b9e62489f5059643fed4916e101dc0dc66730d7f5ebb48df3a137a40d7b6109b9a3ef01e5c03925ad90a92d0b",
        "3d68455ccbb0685f131f1ed7938d3fbeaa6784f8b81efe13a63dd007e7c113c6d7024b22e0148bbf6ba01ed600f1b2cd",
        "15a546dc3994027cd438058f11b903c0d787e6d3a7513957690890b0f8a27fd9ec7a8fa39dd7aabd488c7aa14cb5c23c",
        "022de2be961372624b374e172ff2bc680158372d118566c21908d9d89734c93464763c8cf96853ff73aa9723e0c6c054",
        "cac2ac8a43b3c52fbae36ebd4a0f838b2544d6eec47092146965bf97c1cc3710c933c29613279fd9b5719b474d1c6374",
        "13036867da2977638e4c1c3364055f905c8aefc88ecec42e2b6b45de90362bea160de4fed708eae6a0d796679a135aaa",
        "f8dacc856836c263e9565f1b19e5b047443210a07dac87c71a488e7ed0f8206ce0af991b21d7abfaa8e4bad7302f97ed",
        "7445820e360496f846b0cf68c9e64b1d0819516feb7a8d565b60f6008573eb5a308c2cc359e305f111bb4f15571ff5af",
    ],
    "sha512": [
        "cd0bbe8890a3773a1f392ef1b2515e1967643ed92eb24841657cf7bd42fb105e3ef0a6a77f0b821206b88b06ae0834355f6a058530f728c56ca32ee7deab4763",
        "47f8f32031fd186203ab6f2dc351fc03f9390732d57fd174a4413fe6c32c4cf60c26f86ae07e9ad13e9721cac66dd82caa13f456ae4a083937bf04c60da9a106",
        "2efa3c5947ac23f8f6c0f1261f0f1b43ffd695718fba7490b8792e2d67309c879570f71d0fbe0af7efbe8f4f6f3f2c8f260fe3fd2198fe4c9273c5fe24246cf7",
        "ce0df9c4d1c44db5d4abd457f55f19042f559bb86dffc55098dfd401e2e38cb9a3286d9762e0e28f85f686a5d08f40f79f325c0164b209761232f4bcfeb940f4",
        "3cc01ada4978f8fdc3adfda9ee469e68e7131457150200e91112dfd4cf76c346eb42ceff4400f313215640aa5ca9460255ec7aa4b12c9c7c2b6a2f66c70b4bc5",
        "969035a845fdbfd3f7bc652bb0adfeba3ea604cae92cc98bdabafffd6431fe12e21b165da036246712af99fe75f86d42424de7c2091d510230872303758bd30e",
        "89736e17ec86185d7c8a45998866110c632158bc09bd9a4aa25188afbc8e464f2e33a48b30858acbc7f95fd092fa9749841470a2e73a543e14eb7caacd4dda4c",
        "22b2aaa3c6d6ee5a93c0793ad5fc82d11535c791603ef951f71a52d534261c8bb922f712312ee5261a51ffa7e2ce133d79373bc8d50719f48aebd406bbe72827",
    ],
    "md5": [
        "69d94f9be984466593898bc896b97549",
        "d3b64ffb626f31654a3ba897a5a40d71",
        "d32314e44ca935982d72a9d02e2809e1",
        "93d2309ba256eed2a5d1c20e52bda257",
        "22ca98e12b1d1d5c7c0f39a83f76d818",
        "5ea03e1d5a87d94eee5c2f62b93a8797",
        "6373df0f2fd6aee1a137b5e5b9cc0d8c",
        "f727a3773fda2211d4f586562fab0e95",
    ],
    "crc32": [
        258001534,
        1332775586,
        783940305,
        2461516651,
        179142731,
        914883422,
        1419986439,
        2537042387,
    ],
    "murmur3": [
        -1937594270,
        -1738685226,
        2017441222,
        -1964039533,
        1864722707,
        690812999,
        841867709,
        853362803,
    ],
    "xxhash64/string": [
        "-942242207906623067",
        "-8711574985793415895",
        "5965191501987965608",
        "2377208065479243432",
        "4598369744765628714",
        "-3378535398816028078",
        "-1144582246501955171",
        "3006146523491832284",
    ],
    "sha256/i": [
        "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
        "1bad6b8cf97131fceab8543e81f7757195fbb1d36b376ee994ad1cf17699c464",
        "972dcafa6fb4c2c88bce752fca4ab18c6bd88599330a4ad9813915b05bfbe76d",
        "56bb3b3a6aa1747def7c225256374c5e73f2fc46555adc47ea16e2d782159387",
        "5feceb66ffc86f38d952786c6d696c79c2dbc239dd4e91b46729d73a27fb57e9",
        "7902699be42c8a8e46fbbb4501726517e86b22c56a189f7625a6da49081b2451",
        "2c624232cdd221771294dfbb310aca000a0df6ac8b66b696d90ef06fdefb64a3",
        "a9538a23904f7d4125bd931dba7c76a54705e6acfe56d5c620810d1941f3c861",
    ],
    "sha256/l": [
        "b34a1c30a715f6bf8b7243afa7fab883ce3612b7231716bdcbbdc1982e1aed29",
        "85386477f3af47e4a0b308ee3b3a688df16e8b2228105dd7d4dcd42a9807cb78",
        "5feceb66ffc86f38d952786c6d696c79c2dbc239dd4e91b46729d73a27fb57e9",
        "73475cb40a568e8da8a045ced110137e159f890ac4da883b6b17dc651b3a8049",
        "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
        "7902699be42c8a8e46fbbb4501726517e86b22c56a189f7625a6da49081b2451",
        "2c624232cdd221771294dfbb310aca000a0df6ac8b66b696d90ef06fdefb64a3",
        "a9538a23904f7d4125bd931dba7c76a54705e6acfe56d5c620810d1941f3c861",
    ],
    "sha256/d": [
        "c26617c7ccbcaa6631b45d851b8cf56e21d2ca624bdb1193afdbd4b560702cec",
        "d5b592c05dc25b5032553f1b27f4139be95e881f73db33b02b05ab20c3f9981e",
        "f00840de446b9eb2b3f75e3ac9ffdedc660b61e36815add64e89f7ea99204b5a",
        "3f0e4f9e1ad99aef700a721b00891d06ccfff696ca9592db8f8af3da4444e08b",
        "dc87fa681eabb0acc1da786aee07bf709f5a27e3b1164dae6867ab470941bee2",
        "b119ca90e3d9542f02927edfcb4ccb787ca55977c8165f07ae2268e9624b2d9a",
        "14be4b45f18e0d8c67b4f719b5144eee88497e413709d11d85b096d8e2346310",
        "a9538a23904f7d4125bd931dba7c76a54705e6acfe56d5c620810d1941f3c861",
    ],
    "sha256/dec": [
        "4aa1f2f334be89264a259bc7fffac7239e3bbecac239295e7a222646259d9145",
        "a454c55d740042142b9295f0a8a1309cc2933a20771a22a231730c63328303e0",
        "9747fa0580f75fd00a1de3ab7cdaea8ad73e12ecf63dbfbb7db394fb9626a1ef",
        "be2ab493b744cc2dfb6249030003f939b48f84b77c15f3b215a5429d01c7a2a3",
        "54f81043322cc758b87d6966672c905fdccb9e986e5d45590865e4e87ea55cca",
        "284c87cc5353066ecaa8ee080460f5982319d4c95b20788f61e2726de49e218d",
        "46fe6a4c5d944ae635bcdea957d664be6b3016f24850f59ee3a58d73ec86560b",
        "a9538a23904f7d4125bd931dba7c76a54705e6acfe56d5c620810d1941f3c861",
    ],
    "sha256/dt": [
        "2b65ec693644068605c58315fc62d32e4eff6b2f515de973ce63f5bc6e3dcadf",
        "85c14296d9598554eeb207f773a614a81cdefaecbf35a0d7051f27cf07f896b3",
        "524be55b2827968f281708f4173aa7344da4124cb13ad591a19b1920c4f160e6",
        "495ac5d03abc7e2812998371d1c9d8f802f1995fe9ab2135a30578852add5c44",
        "41b62fb4518505d36dcd35c683efe1310d24ea22d6d146a0804c818070531814",
        "41b62fb4518505d36dcd35c683efe1310d24ea22d6d146a0804c818070531814",
        "41b62fb4518505d36dcd35c683efe1310d24ea22d6d146a0804c818070531814",
        "a9538a23904f7d4125bd931dba7c76a54705e6acfe56d5c620810d1941f3c861",
    ],
    "sha256/ts": [
        "3acfe3c40e11ec7df23880706e7bf93c9659f7c23f60db0ae2e6684df45d6f84",
        "600b7412fff01e4c4f4bea1896e1fedf56ee29d3522835ae6a98aa0898d1bef1",
        "78255555decefad199a059d0e3af5453afcb191f8c7a444fd0f6b385f8a57c78",
        "cf18a9f070bbded5e981998ac4752c3609a6c1326ea71d515dc5cbf46eb5d126",
        "fcc83d1f6e83a15e58ab4d298428cd7bb48862b380a79c8d4420033ae4ef8c1d",
        "e00d1d8d63f7575381cc858df1c56ea10f0036131dce6fa0d18ea48f8eee7dae",
        "23658861770ca33eecebdef9c059d85136b76efa64a64bbd719ddee9957622a6",
        "a9538a23904f7d4125bd931dba7c76a54705e6acfe56d5c620810d1941f3c861",
    ],
    "sha256/b": [
        "b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b",
        "fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa",
        "b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b",
        "fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa",
        "b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b",
        "b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b",
        "b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b",
        "a9538a23904f7d4125bd931dba7c76a54705e6acfe56d5c620810d1941f3c861",
    ],
    "sha256/s": [
        "377588a7083f4d82c547c92e7a73bcf56b7922e6667bf75ef578c0f68409feb8",
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "fcbb3c58e90dce60fdf595f69c4ff76f307995fd461219fd7d50383a84571338",
        "a9538a23904f7d4125bd931dba7c76a54705e6acfe56d5c620810d1941f3c861",
        "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
        "3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d",
        "2e7d2c03a9507ae265ecf5b5356885a53393a2029d241394997265a1a25aefc6",
        "a9538a23904f7d4125bd931dba7c76a54705e6acfe56d5c620810d1941f3c861",
    ],
    "sha256/bin": [
        "06eb7d6a69ee19e5fbdf749018d3d2abfa04bcbd1365db312eb86dc7169389b8",
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "5f78c33274e43fa9de5659265c1d917e25c03722dcb0b8d27db8d5feaa813953",
        "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
        "620bfdaa346b088fb49998d92f19a7eaf6bfc2fb0aee015753966da1028cb731",
        "4bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a",
        "dbc1b4c900ffe48d575b5da5c638040125f65db0fe3e24494b76ea986457d986",
        "a9538a23904f7d4125bd931dba7c76a54705e6acfe56d5c620810d1941f3c861",
    ],
}


@pytest.mark.spark
def test_hash_output_matches_frozen_goldens(spark: SparkSession) -> None:
    """Hash keys are bit-identical to the frozen goldens (per algorithm and per column)."""
    actual = compute_hashes(spark)
    changed = sorted(k for k in GOLDENS if actual.get(k) != GOLDENS[k])
    assert not changed, f"hash output changed for: {changed}"
    assert actual.keys() == GOLDENS.keys()

# Memcached 実験（本番環境向け手順メモ）

このドキュメントは、Orthrus リポジトリの memcached 実験を **本番環境（SUT）** で実行し、**別ホスト（Load host）** から負荷をかける前提での手順をまとめたものです。

対象は「スループット比較（vanilla / SEI(複数variant) / Orthrus(同期/非同期) / RBV）」と、そのための **RPS sweep** です。

---

## 0. 構成（2ホスト）

- **SUT（Server host）**: memcached サーバ（各 variant）を起動するホスト
- **Load host（Client host）**: `memcached_client` を動かして負荷を生成するホスト
- 実行は **SUT 上で** `scripts/memcached/run-compare.py` / `run-sweep-*.py` を動かし、必要なときだけ SSH 経由で Load host にクライアントを起動します。

重要:
- `memcached_client` はホスト名解決をしないため、接続先は **IPv4 文字列**（例: `10.0.0.10`）を使います（`--server-ip`）。
- サーバは `INADDR_ANY` に bind するので、SUT の外部インタフェースから接続可能です（FW/NAT の許可は必要）。

---

## 1. 事前準備（SUT / Load host 共通）

### 必要なもの（最低限）

- Linux (x86_64 想定)
- `gcc`（GCC TM を使うため。clang ではなく GCC 推奨）
- `make`, `ar`, `cmake (>= 3.20)`, `python3`
- （任意）`ninja`（CMake の generator として）
- SSH 接続（SUT → Load host）

Orthrus 側の一般的な依存関係は `docs/prerequisite.md` も参照してください。

---

## 2. libsei-gcc の配置

Orthrus の memcached(SEI) は `../libsei-gcc` を参照します（`ae/memcached/CMakeLists.txt`）。

ディレクトリ構成例:

```text
<WORKDIR>/
  Orthrus/
  libsei-gcc/
```

もし `libsei-gcc` を別パスに置きたい場合は、`../libsei-gcc` になるように **シンボリックリンク**を張るのが簡単です。

---

## 3. libsei-gcc の variant（build）のコンパイル方法

Orthrus の memcached(SEI) は、libsei-gcc の **複数 build ディレクトリ**をリンクします。
同じ build dir を別フラグで再利用するとオブジェクトが混ざる可能性があるため、各 build dir は固定用途にし、作り直すときは `clean` を入れてください。

### 対応表（Orthrus の `--sei-variant` と libsei-gcc ビルド）

| `--sei-variant` | Orthrus が参照する build dir | libsei-gcc の Make 変数（例） |
|---|---|---|
| `default` | `build` | （デフォルト: `EXECUTION_REDUNDANCY=2` 相当） |
| `er2` | `build_er2_nomig` | `EXECUTION_REDUNDANCY=2`（ROLLBACK 無し） |
| `er5` | `build_er5_nomig` | `EXECUTION_REDUNDANCY=5`（ROLLBACK 無し） |
| `er10` | `build_er10_nomig` | `EXECUTION_REDUNDANCY=10`（ROLLBACK 無し） |
| `dynamicNway` | `build_dyn_nway_er5_rb` | `ROLLBACK=1 EXECUTION_REDUNDANCY=5` |
| `dynamicCore` | `build_dyn_core_rb` | `ROLLBACK=1` |

### まとめてビルドする例（SUT 側で実行）

```bash
cd ../libsei-gcc

# default
make BUILD=build clean all

# static N-way (no rollback)
EXECUTION_REDUNDANCY=2  make BUILD=build_er2_nomig  clean all
EXECUTION_REDUNDANCY=5  make BUILD=build_er5_nomig  clean all
EXECUTION_REDUNDANCY=10 make BUILD=build_er10_nomig clean all

# dynamic variants (rollback enabled)
ROLLBACK=1 EXECUTION_REDUNDANCY=5 make BUILD=build_dyn_nway_er5_rb clean all
ROLLBACK=1 make BUILD=build_dyn_core_rb clean all
```

補足:
- 特定の GCC を使いたい場合は `CC=gcc-12` のように指定できます（例: `CC=gcc-12 ROLLBACK=1 make BUILD=... clean all`）。

---

## 4. Orthrus（memcached）のビルド（SUT 側）

memcached だけが目的なら、依存を減らすために Phoenix/Masstree/LSMTree を OFF にしても構いません。

例（Release + Ninja）:

```bash
cd Orthrus
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DENABLE_LSMTREE=OFF -DENABLE_PHOENIX=OFF -DENABLE_MASSTREE=OFF
cmake --build build -j
```

成果物（例）:

```text
build/ae/memcached/
  memcached_vanilla
  memcached_orthrus
  memcached_orthrus_sync
  memcached_rbv_primary
  memcached_rbv_replica
  memcached_sei
  memcached_sei_er2
  memcached_sei_er5
  memcached_sei_er10
  memcached_sei_dynamic_nway
  memcached_sei_dynamic_core
  memcached_client
```

---

## 5. Load host の準備（別ホストから負荷をかける）

Orthrus のリモート負荷モードでは、Load host 側で `memcached_client` が実行できれば OK です。
（Load host で Orthrus をフルビルドする必要はありません。）

### 最小構成（バイナリだけコピー）

SUT でビルドした `memcached_client` を Load host にコピーします:

```bash
# SUT から実行
ssh user@loadhost "mkdir -p /tmp/orthrus-memcached"
scp build/ae/memcached/memcached_client user@loadhost:/tmp/orthrus-memcached/memcached_client
ssh user@loadhost "chmod +x /tmp/orthrus-memcached/memcached_client"
```

以降、実験時に `--remote-client-bin /tmp/orthrus-memcached/memcached_client` を指定します。

---

## 6. ネットワーク / FW 設定（重要）

`run-compare.py` は **ngroups 個の連続ポート**（例: `base..base+ngroups-1`）を使います。
本番環境で FW が厳しい場合は、スクリプトにポート範囲を与え、その範囲を Load host から SUT に対して許可してください。

- 推奨: `--port-start 20000 --port-end 21000` のように **十分広い範囲**を確保
- `--server-ip` は Load host から到達できる SUT の IPv4 を指定

---

## 7. 実験の実行（SUT から）

### 7.0 公平性（サーバコア数を揃える）

「どの variant でも使用するコア数を揃えて比較したい」場合は、`--preset fair4c` を使います。

- `scripts/memcached/run-compare.py` の `--preset` はデフォルト `default` なので、**公平比較したい場合は明示的に `--preset fair4c` を付ける**のが安全です。
- `--pin` はデフォルト有効です（`taskset` で pin します）。`--no-pin` にすると OS スケジューリング任せになり、厳密な比較が崩れます。
- `--preset fair4c` のとき、サーバ側の CPU 割当は概ね次の通りです（合計コア数を揃える設計）:
  - vanilla / SEI: `server4`（最大4コア）
  - Orthrus / Orthrus(sync): `server4` と同じ4コアに pin した上で、内部的に **work=3コア + validation=1コア** になるように `SCEE_WORK_CPUSET` 等を設定
  - RBV: `server4` を primary/replica に分割して **合計4コア**（例: 2+2）
- 実際の割当は `run-compare.py` 実行時に `CPU layout: server4=...` と stderr に出るので、そこで確認できます。

どの CPU コア（番号）を `server4` に使うかは、`run-compare.py` 自身の CPU アフィニティ（`os.sched_getaffinity(0)`）に依存します。固定したい場合は、実験を起動する前に `taskset` / cpuset で Python のアフィニティを制限してください（例: `taskset -c 4-47 python3 scripts/memcached/run-sweep-rps.py --preset fair4c ...`）。

別ホスト負荷（`--client-ssh`）時のクライアント側 pin は、必要なら `--client-pin-cpus` で明示します。

### 7.1 単発の比較（throughput）

```bash
python3 scripts/memcached/run-compare.py \
  --mode throughput \
  --preset fair4c \
  --server-ip <SUT_IPV4> \
  --port-start 20000 --port-end 21000 \
  --sei-variant er5 \
  --client-ssh user@loadhost \
  --remote-client-bin /tmp/orthrus-memcached/memcached_client \
  --client-pin-cpus 0-7 \
  --tag prod.throughput.er5
```

### 7.2 RPS sweep（おすすめ）

```bash
python3 scripts/memcached/run-sweep-rps.py \
  --preset fair4c \
  --server-ip <SUT_IPV4> \
  --port-start 20000 --port-end 21000 \
  --client-ssh user@loadhost \
  --remote-client-bin /tmp/orthrus-memcached/memcached_client \
  --client-pin-cpus 0-7 \
  --sei-variants er2,er5,er10,dynamicNway,dynamicCore \
  --nclients 16 \
  --rps 0,2000,4000,8000,12000 \
  --repeats 3 \
  --tag-prefix prod_sweep_rps.2025XXXX
```

まずは `--dry-run` でコマンドを確認するのがおすすめです:

```bash
python3 scripts/memcached/run-sweep-rps.py --dry-run ...
```

---

## 8. 出力（results/）

- 単発比較: `results/memcached-throughput-report.<tag>.txt` と `results/memcached-throughput-report.<tag>.txt.json`
- sweep 集計: `results/memcached-throughput-vs-rps.<out-tag>.{json,csv,svg}`

---

## 9. よくある詰まりポイント

- Load host から接続できない:
  - `--server-ip` が間違っている / ルーティングできていない / FW で落ちている
  - `--port-start/--port-end` の範囲が閉じている
- `memcached_client` が実行できない:
  - `--remote-client-bin` のパスが違う、実行権限が無い
- ポート確保に失敗する:
  - 範囲が狭い or 他プロセスが使用中 → `--port-end` を広げる / 使用状況を整理
- CPU pin が合わない:
  - `--client-pin-cpus` を外してまず動作確認（サーバ pin は `--pin/--no-pin` で制御）

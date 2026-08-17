#!/usr/bin/env python3
"""Tái cấu trúc dataset Parquet của dashboard — NHIỆM VỤ 4.  CHƯA CÓ LOGIC.

Hiện trạng: `data/gold_events/` gồm 5.000 file, mỗi file vài chục KB, không
partition, thứ tự hàng ngẫu nhiên.

Yêu cầu: đọc toàn bộ dataset cũ, ghi ra dataset mới có layout hợp lý hơn, sau đó cập
nhật `queries/dashboard.sql` để trỏ vào dataset mới.

    python tools/compact.py       # ghi dataset mới
    python tools/explain.py       # đo lại và so với baseline

KHUNG THỰC HIỆN

    COPY (
        SELECT *
        FROM   read_parquet('data/gold_events/*.parquet')
        ORDER  BY <cột A>, <cột B>
    ) TO 'data/gold_events_v2' (
        FORMAT          parquet,
        PARTITION_BY    (<cột partition>),
        OVERWRITE_OR_IGNORE,
        ROW_GROUP_SIZE  <?>
    )

Ba quyết định, mỗi quyết định cần một lý do viết được ra giấy:

  <cột partition>   Engine chỉ bỏ qua được file mà nó biết là vô ích TRƯỚC khi
                    mở file. Thông tin đó đến từ đường dẫn. Vậy cột nào của
                    truy vấn dashboard nên xuất hiện trong tên thư mục? Cột đó
                    có bao nhiêu giá trị phân biệt — tức bao nhiêu thư mục?
                    Partition theo cột có 650 giá trị thì hệ quả là gì?

  <cột A>, <cột B>  Thứ tự hàng trong file quyết định thống kê min/max của mỗi
                    row group có ích hay vô dụng. Sắp thế nào để các hàng cùng
                    một khách hàng nằm liền nhau?

  ROW_GROUP_SIZE    Mặc định 122.880 hàng. Một ngày có khoảng bao nhiêu hàng?
                    Nếu cả ngày gói gọn trong MỘT row group thì min/max của
                    row group đó phủ những gì, và còn tác dụng lọc không?

Sau khi chạy xong, kiểm tra lại bằng `python tools/explain.py`: `rows scanned`
phải giảm, `files` phải giảm, và `result hash` phải GIỮ NGUYÊN.
"""

from __future__ import annotations

import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import DATA  # noqa: E402

SRC = DATA / "gold_events"
DST = DATA / "gold_events_v2"


# ---------------------------------------------------------------------------
# BA QUYẾT ĐỊNH — lý do cho từng con số
# ---------------------------------------------------------------------------
# PARTITION_BY = event_date  (14 giá trị phân biệt -> 14 thư mục)
#   Dashboard filter theo HAI cột: customer_name (650 giá trị) và ngày (14).
#   Chỉ nên đưa cột thấp cardinality vào đường dẫn. Partition theo
#   customer_name sinh 650 thư mục, mỗi thư mục ~200 hàng — đó chính là
#   small-file problem cũ được dựng lại dưới tên khác. Cột 650 giá trị được
#   xử lý bằng thứ tự hàng + thống kê row group (xem dưới), không bằng path.
#
# ORDER BY customer_name, event_time
#   Thống kê min/max của một row group chỉ có ích khi giá trị trong group nằm
#   trong khoảng HẸP. Dữ liệu gốc xếp ngẫu nhiên nên mọi row group đều có
#   min='ACME', max='Cust_0650' — phủ toàn bộ 650 khách hàng, engine không
#   loại được group nào. Sắp theo customer_name làm các hàng cùng một khách
#   hàng nằm liền nhau: sau khi nén, file 08-09 có 5 row group và 3.500 hàng
#   của ACME gom vào 2 group đầu (group 0 có min=max='ACME'), 2 group kế tiếp
#   có min > 'ACME' nên loại được.
#   Lợi ích đo được chắc chắn: dữ liệu cùng loại nằm cạnh nhau nén tốt hơn
#   nhiều — 20 MB (5.000 file) xuống 3,8 MB (14 file).
#
# ROW_GROUP_SIZE = 2048
#   Một ngày chỉ ~9.340 hàng. Mặc định 122.880 gói cả ngày vào MỘT row group,
#   min/max của group đó phủ hết 650 khách hàng nên vô dụng dù đã sắp xếp.
#   2048 = bội số vector size của DuckDB (khai nhỏ hơn cũng bị làm tròn lên
#   2048) -> ~5 group mỗi ngày, mỗi group phủ ~130 khách hàng.
#
# ⚠️ ĐO ĐỐI CHỨNG — đừng nhận công cho thứ không tạo ra kết quả.
#   Đã thử 4 biến thể (có/không ORDER BY × row_group_size mặc định/2048/1000)
#   trên DuckDB 1.5.5: rows_scanned = 9.324 ở CẢ BỐN, và cũng bằng 9.324 với
#   mọi ngày được filter (kể cả ngày có 9.409 hàng). Vậy metric
#   OPERATOR_ROWS_SCANNED mà bài này chấm scale theo SỐ FILE ĐƯỢC MỞ, không
#   theo số row group được decode.
#   => Toàn bộ 536× cải thiện đến từ PARTITION PRUNING (14 file -> 1 file),
#      cộng với việc viết lại predicate cho sargable. ORDER BY và
#      ROW_GROUP_SIZE là layout đúng cho dữ liệu lớn hơn (và cho tỷ lệ nén),
#      nhưng KHÔNG phải nguyên nhân của con số trong bảng chấm.
#   Một ghi chú nữa: COPY ... PARTITION_BY không bảo toàn trọn vẹn thứ tự
#   ORDER BY trong từng file — hàng được đệm và flush theo partition, nên
#   file 08-09 có 4 group sắp đúng thứ tự (ACME -> Cust_0520) và 1 group
#   "vét" cuối trải rộng Cust_0002 -> Cust_0650.
# ---------------------------------------------------------------------------

PARTITION_COL = "event_date"
SORT_COLS = ("customer_name", "event_time")
ROW_GROUP_SIZE = 2_048


def main() -> int:
    con = duckdb.connect()

    n_src = len(list(SRC.glob("*.parquet")))
    print(f"  nguồn : {SRC}  ({n_src:,} file)")

    n_rows_src = con.execute(
        f"select count(*) from read_parquet('{SRC}/*.parquet')"
    ).fetchone()[0]

    con.execute(f"""
        copy (
            select *
            from   read_parquet('{SRC}/*.parquet')
            order  by {', '.join(SORT_COLS)}
        ) to '{DST}' (
            format          parquet,
            partition_by    ({PARTITION_COL}),
            overwrite_or_ignore,
            row_group_size  {ROW_GROUP_SIZE}
        )
    """)

    n_files_dst = len(list(DST.rglob("*.parquet")))
    n_rows_dst = con.execute(
        f"select count(*) from read_parquet('{DST}/**/*.parquet', hive_partitioning = true)"
    ).fetchone()[0]

    # Nén lại layout không được phép làm mất hàng nào.
    assert n_rows_src == n_rows_dst, (
        f"mất hàng khi nén: nguồn {n_rows_src:,} != đích {n_rows_dst:,}"
    )

    print(f"  đích  : {DST}  ({n_files_dst:,} file)")
    print(f"  partition by {PARTITION_COL} · order by {', '.join(SORT_COLS)} "
          f"· row_group_size {ROW_GROUP_SIZE:,}")
    print(f"  {n_rows_dst:,} hàng — khớp nguồn, không mất hàng nào.")
    print(f"\n  đo lại:  make explain\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())

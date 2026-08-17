# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Lê Nguyễn Minh Quang · **MSSV:** 2A202601248 · **Lớp:** AICB-P2T2 · **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details>
<summary>Output ba lượt chạy</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 16.4s
  run 2/3 … 21.2s
  run 3/3 … 18.0s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    f8d3f591f0    f8d3f591f0    f8d3f591f0   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt** · hai bài mở rộng: **A đạt** (536,3×) · **B đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Phiếu #1041: sau khi bấm Clear Task trên Airflow, `gold_training_set` phình ra; chạy lại lần nữa lại phình thêm, không có log lỗi nào. Đo trực tiếp bằng `make reset` rồi `make pipeline` hai lượt: lượt 1 = 13.790 hàng, lượt 2 = 26.270 — cộng thêm đúng 12.480; lượt 3 = 38.750 theo trạng thái ban đầu của `make verify`, khớp quy luật đó. |
| **Nguyên nhân** | Model khai `materialized='incremental'` nhưng **không khai `unique_key`**, nên dbt không có khoá để đối sánh và generate ra câu `INSERT INTO … SELECT` thuần. Với câu lệnh đó, chạy lại cùng một partition ngày là **ghi thêm**, không phải ghi đè — mỗi lượt chạy lại là một lần nhân bản toàn bộ dữ liệu, và vì không ai lỗi nên không có gì báo. Tệ hơn: `silver_tickets` là bảng entity mang `_ingested_at` của **lần cập nhật mới nhất**, nên một ticket tạo ngày D1 rồi update ngày D5 lọt qua mệnh đề `WHERE … run_date` ở **hai ngày khác nhau ngay trong một lượt chạy** (D1 với `_ingested_at=D1`, rồi D5 sau khi silver được dựng lại) — đó là 1.310 hàng thừa của lượt đầu, đúng bằng số bản ghi `op='u'`. Cơ chế cần nhớ: **grain của bảng là entity, còn điều kiện lọc lại theo thời gian nạp; hai thứ đó không cùng đơn vị, nên chỉ khoá theo entity mới khử được trùng.** |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql` — thêm `unique_key='ticket_id'` và `incremental_strategy='merge'` vào `config()`. Giữ nguyên mệnh đề `WHERE` theo `run_date`. `dags/ai_training_pipeline.py` — `catchup=False`, `max_active_runs=1`. |
| **Bằng chứng** | trước: 13.790 → 26.270 → 38.750 hàng · sau: **12.480** ở cả ba lượt · checksum 3 lượt: `8dd7c98653` giống hệt nhau · `gold_training_set: 1 hàng / 1 ticket ✓` · SQL dbt sinh ra đổi từ `INSERT INTO` thành `MERGE … ON (SOURCE.ticket_id = DEST.ticket_id)` |

**Vì sao `merge` mà không phải `delete+insert` theo partition ngày?** `delete+insert` xoá ở đích những hàng thuộc partition của batch hiện tại. Bản sao cũ của một ticket được update lại nằm ở **partition ngày D1**, còn bản ghi mới thuộc **partition D5** — xoá partition D5 không chạm tới bản sao ở D1, nên 1.310 hàng thừa vẫn còn. Grain là entity, natural key là `ticket_id`, nên phải khoá theo key chứ không theo partition.

**Về hai tham số DAG.** `catchup=True` khiến một lần bật/clear DAG sinh ra 14 run dồn một lúc; `max_active_runs` không đặt cho phép nhiều run ghi đồng thời vào cùng một bảng. Nhưng cả hai chỉ **giảm tần suất kích hoạt**, không phải nguyên nhân: sửa DAG mà không sửa `config()` của model thì `make verify` vẫn đỏ, vì phép ghi tự nó vẫn không idempotent.

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Phiếu #1043: `gold_feature_daily` thiếu ~5% so với đối chiếu thủ công, và chỉ thiếu ở những ngày đã chạy xong từ lâu. Đo được: 8.645 / 9.100 hàng, thiếu 455 cặp `(event_date, customer_id)`, toàn bộ nằm ở 08-03 → 08-13; ba ngày mới nhất không thiếu cặp nào. |
| **P99 độ trễ đo được** | **2,726 ngày** (p50 = 0,128 · p95 = 1,814 · max = 2,945 · 5,05% bản ghi tới kho muộn hơn 1 ngày) |
| **Lookback đã chọn** | **3 ngày** — vì P99 = 2,726 ngày, làm tròn lên là 3. Kiểm tra thêm độ trễ theo **lịch** (`ingested_date − event_date`): 84,09% / 10,94% / 2,97% / 2,00% cho 0/1/2/3 ngày — tối đa đúng 3 ngày, nên 3 phủ hết. |
| **Nguyên nhân** | Điều kiện lọc dùng `event_date > (select max(event_date) from {{ this }})`: mốc so sánh là một watermark theo **thời điểm sự kiện xảy ra**, trong khi dữ liệu về kho theo **thời điểm được nạp**. Watermark đó chỉ tăng đơn điệu, nên bất kỳ event nào tới sau khi ngày của nó đã được xử lý đều rơi ra ngoài filter — và vì mốc không bao giờ lùi lại, nó **mất vĩnh viễn**, không phải trễ rồi được bù. Cụ thể: event `event_date=08-12`, `_ingested_at=08-15`; tại lượt 08-15 đích đã chứa tới 08-14 nên filter chỉ nhận `> 08-14`, event bị loại; ngày 08-16 mốc thành 08-15, càng loại chắc. Cơ chế cần nhớ: **watermark của incremental phải đặt trên đại lượng mà dữ liệu đến theo, hoặc phải chừa một biên bằng độ trễ thực đo được; đặt watermark trên event time mà không có biên là mặc định rằng dữ liệu không bao giờ về muộn.** |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql` — đổi filter thành `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day`, và thêm `unique_key=['event_date','customer_id']` + `incremental_strategy='merge'` để 4 lần tính lại cùng một cặp thay thế nhau thay vì cộng dồn. |
| **Bằng chứng** | trước: 8.645 hàng (thiếu 455) · sau: **9.100** hàng · checksum 3 lượt `f8d3f591f0` giống hệt nhau · `gold_training_set` giữ nguyên 12.480 |

**Thiệt hại ẩn mà cột `SỐ HÀNG` không lộ ra.** Đối chiếu từng cặp với `silver_events`, trước khi sửa còn có **5.746 cặp đã tồn tại trong Gold nhưng `n_events` bị hụt** — event về muộn của những cặp đó không được gộp vào, và vì hàng đã có nên không có gì thiếu để đếm. Sau khi sửa: 9.100 cặp, `cap_sai_n_events = 0`, `cap_sai_tokens = 0`. Đây là lý do bảng có thể `ỔN ĐỊNH ✓` mà vẫn sai: ổn định chỉ nói "chạy lại cho cùng kết quả", không nói "kết quả đúng".

**Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí mỗi lựa chọn là gì?**

> P99 là thống kê **bền**: nó không bị một outlier đơn lẻ kéo dài. `max` là thống kê **giòn** — một message kẹt 30 ngày sẽ buộc window thành 30 ngày, và cái giá đó phải trả ở **mọi lượt chạy về sau**, mãi mãi, chỉ vì một bản ghi. Mỗi ngày lùi thêm là một partition ngày phải đọc lại và tính lại ở mỗi lượt, tức chi phí thường trực chứ không phải một lần.
>
> Cách làm đúng là lấy P99 rồi làm tròn lên, sau đó **kiểm tra xem con số đó có phủ `max` hay không** — ở đây P99 = 2,726 → 3 ngày, và `max` = 2,945 ngày cũng nằm trong 3 ngày, nên không mất bản ghi nào. Nếu `max` vượt xa window đã chọn thì phần đuôi đó không được im lặng bỏ qua mà phải xử lý bằng cơ chế khác: một job backfill riêng theo `_ingested_at`, hoặc alert khi có bản ghi tới muộn hơn window. Chọn window theo P99 là quyết định về **chi phí thường trực**, không phải sự cho phép làm mất dữ liệu.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Phiếu #1047: backend đổi kiểu `priority` từ số sang chuỗi hôm 08-10, pipeline không hề dừng, `dbt test` vẫn 9/9 pass, nhưng model phân loại từ hôm đó dự đoán kém hẳn. Đo được: `silver_tickets.priority` có **6.488 NULL** (52% toàn bảng), đồng thời có cả `0`, `5`, `-1` — trong khi contract quy định 1..4. |
| **Nguyên nhân** | Tầng Silver chuẩn hoá `priority` bằng đúng một biểu thức `try_cast(priority_raw as integer)`. Khi nguồn thực hiện **schema evolution** (số → nhãn chữ) từ 08-10, `try_cast` thất bại và trả về **NULL một cách im lặng** — đó là hành vi thiết kế của `try_cast`, nên không có exception, không có test nào đỏ, không có ai được báo. Sai lệch chỉ hiện ra ở đầu ra của mô hình machine learning, cách nguồn lỗi hai tầng. Đồng thời `try_cast` sai theo hướng **ngược lại** với `0`, `5`, `-1`: chúng đúng là integer nên đi qua sạch sẽ dù nằm ngoài miền contract. Contract bị tắt (`enforced: false`) là lý do không có chốt nào chặn được cả hai hướng sai này. Cơ chế cần nhớ: **mọi phép chuyển kiểu "an toàn" đều là một điểm mất dữ liệu im lặng nếu không có nơi tiếp nhận phần thất bại; và một cột đặc trưng hỏng không làm pipeline đỏ, nó chỉ làm mô hình tệ đi — nên phải bắt bằng contract + test, chứ không đợi triệu chứng.** |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **Nhóm 1 — số hợp lệ** `1 2 3 4` (6.846 bản ghi): đúng contract ban đầu → **giữ nguyên**. **Nhóm 2 — nhãn chuỗi** `urgent high medium low` (7.142 bản ghi = **49,9%** toàn bộ CDC): đây là **schema evolution**, nguồn đổi cách biểu diễn nhưng ý nghĩa không đổi → **map** về 1/2/3/4 theo tài liệu API. **Nhóm 3 — giá trị không hợp lệ** `0 5 -1 P1 P2 unknown '' NULL` (**312** bản ghi): dữ liệu lỗi thật → **quarantine**. Tiêu chí phân biệt nhóm 2 với nhóm 3: *giá trị này có mang đúng thông tin của contract cũ, chỉ khác cách biểu diễn hay không?* `urgent` có; `P1` thì không suy ra được nó là 1 hay là nhãn nội bộ nào khác. Quarantine cả nhóm 2 là vứt đi gần một nửa dữ liệu hợp lệ chỉ vì nguồn đổi format. |
| **Cách khắc phục** | **(a)** `dbt/macros/normalize_priority.sql` — thay `try_cast` bằng khối `CASE` xử lý đủ ba nhóm, trả `NULL` cho nhóm 3 làm tín hiệu "không hợp lệ". Nhóm 1 dùng `try_cast(...) between 1 and 4` chứ không phải `is not null`, nhờ đó `0/5/-1` rơi xuống `else`. Macro được **cả hai** model dùng nên chúng không thể lệch nhau. **(b)** `dbt/models/silver/silver_tickets.sql` — cấu trúc lại thành `normalized → valid → ranked → latest`, **lọc bản ghi hỏng TRƯỚC khi `row_number()`** (xem ghi chú bên dưới bảng). **(c)** `dbt/models/silver/quarantine_tickets.sql` — `where {{ normalize_priority('priority_raw') }} is null`. **(d)** `dbt/models/silver/schema.yml` — `contract.enforced: true`, thêm test `not_null` + `accepted_values: [1,2,3,4]` cho `priority`. Làm thêm: macro `priority_reject_reason` phân loại 4 lý do bị loại để người trực đọc log là biết phải làm gì. |
| **Bằng chứng** | `quarantine_tickets` = **312** hàng (đúng grain 1 hàng / 1 bản ghi CDC), checksum 3 lượt `ebb89036fb` · `dbt test` **11/11 pass** (bản gốc 9) · `silver_tickets.priority`: 1→3.134, 2→3.029, 3→3.115, 4→3.202, **0 NULL** · `silver_tickets` vẫn **12.480** ticket · phân hoạch trọn vẹn: 14.300 bản ghi CDC = 13.988 hợp lệ + 312 quarantine · contract chặn thật: thử đổi `data_type` thành `varchar` thì dbt fail hẳn model với `data type mismatch` (đã revert) |

**Thứ tự lọc/xếp hạng — cái bẫy quyết định số hàng.** Nếu chỉ thêm `where priority is not null` vào cuối file (tức xếp hạng trước, lọc sau), thì ticket nào có bản ghi **mới nhất** bị hỏng sẽ biến mất hoàn toàn khỏi Silver: số ticket tụt 12.480 → 12.168, và `gold_training_set` hụt theo. Đúng phải là lọc trước, xếp hạng sau — ta loại **bản ghi** hỏng, không loại cả **ticket**, vì ticket đó vẫn còn một trạng thái hợp lệ từ lần cập nhật trước. Một bản ghi CDC lỗi làm mất một lần cập nhật, nó không được phép làm mất cả thực thể.

**Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao không để pipeline dừng khi gặp bản ghi lỗi?**

> **Chặn ở Silver, không chặn ở Bronze.** Bronze phải là bản sao trung thực của nguồn — append-only, không phán xét. Nếu Bronze từ chối bản ghi lỗi thì bằng chứng bị phá huỷ ngay tại cửa vào: không còn cách trả lời "nguồn thực sự gửi cái gì lúc 08-10", không replay lại được sau khi sửa logic chuẩn hoá, và đến lúc phát hiện `P1` thực ra là một nhãn hợp lệ thì dữ liệu đã không còn để backfill. Đúng chỗ để phán xét là Silver, nơi contract được phát biểu; `quarantine_tickets` giữ nguyên `priority_raw` gốc kèm `reject_reason` để điều tra. Nói cách khác: **Bronze trả lời câu hỏi "nguồn đã gửi gì", Silver trả lời "cái gì dùng được" — gộp hai câu hỏi vào một tầng là mất khả năng điều tra.**
>
> **Không dừng pipeline, vì tỷ lệ không cho phép.** 312 bản ghi lỗi (2,2% số bản ghi CDC) không có quyền chặn 13.988 bản ghi CDC hợp lệ, 130.683 event và 31.200 chunk đang chờ tới tay người dùng. Dừng DAG biến một lỗi dữ liệu cục bộ ở một cột thành sự cố mất dịch vụ toàn hệ thống — và vẫn không sửa được gì, vì dữ liệu lỗi nằm ở nguồn, pipeline dừng lại không làm nó hợp lệ trở lại. Mô hình đúng là **định tuyến, không phải chốt chặn**: bản ghi lỗi rơi vào `quarantine_tickets` như một hàng đợi có chủ, phần còn lại chạy tiếp; `dbt test` bảo vệ **bất biến của bảng đã làm sạch** (sau khi lọc thì `priority` phải không NULL và thuộc 1..4) — nó phải đỏ khi *logic của ta* sai, không phải khi *nguồn* gửi rác. Rác từ nguồn là việc bình thường và đã có nơi chứa.

---

## 4 · Bài mở rộng (EXTRA.md)

### Bài A — Query dashboard chậm

| | |
|---|---|
| **Triệu chứng** | Phiếu #1052: dashboard CSKH load mất 38 giây, ba tháng trước chỉ 2 giây, không ai sửa dòng code nào. Đo được: `rows scanned` = 5.000.000 cho một tập chỉ có 130.683 hàng thật, nằm rải trong **5.000 file** Parquet (20 MB). |
| **Nguyên nhân** | Hai lỗi cộng dồn, và cả hai đều là chuyện **layout trên đĩa** chứ không phải chuyện query. **(1) Small-file problem:** DuckDB đọc Parquet theo lô và làm tròn lên theo từng file — một file vài chục hàng vẫn tốn công quét tương đương ~1.000 hàng, nên 5.000 file tí hon tốn 5.000.000 đơn vị công cho 130.683 hàng thật (gấp 38 lần). Query không hề chậm đi; **số file tăng dần theo thời gian** mới là thứ chậm đi, và nó tăng mà không ai commit gì cả — đó chính là lý do "không ai sửa dòng nào" mà vẫn từ 2 giây thành 38 giây. **(2) Predicate không sargable:** filter viết là `strftime(event_time,'%Y-%m-%d') = '2026-08-09'`, cột bị bọc trong một function call. Engine không so được **kết quả của một hàm** với tên thư mục partition hay với min/max statistics của row group, nên buộc phải mở toàn bộ file rồi mới biết file nào có ích. Cơ chế cần nhớ: **engine chỉ bỏ qua được thứ nó biết là vô ích TRƯỚC khi mở file, và thông tin đó chỉ đến từ đường dẫn — mọi hàm bọc quanh cột đều làm mù khả năng đó.** |
| **Cách khắc phục** | `tools/compact.py` — `COPY … TO 'data/gold_events_v2' (format parquet, partition_by (event_date), overwrite_or_ignore, row_group_size 2048)` với `order by customer_name, event_time`, kèm `assert` số hàng nguồn = đích. `queries/dashboard.sql` — trỏ vào dataset mới, bật `hive_partitioning = true`, viết lại filter thành `event_date = date '2026-08-09'` (tương đương về ngữ nghĩa: đã kiểm 0 hàng lệch giữa `event_time::date` và `event_date`). |
| **Bằng chứng** | `rows scanned` **5.000.000 → 9.324** (giảm **536,3×**, cần ≥ 10×) · `files` **5.000 → 14** · `result hash` `4379e4c5d9f3` **không đổi** · dung lượng 20 MB → 3,8 MB · thời gian tham khảo 9,8 ms |

**Ba quyết định layout, và một phép đo đối chứng.** `partition_by = event_date` vì dashboard filter theo hai cột nhưng chỉ nên đưa cột **thấp cardinality** vào đường dẫn: 14 thư mục là hợp lý, còn partition theo `customer_name` sẽ sinh 650 thư mục mỗi cái ~200 hàng — đúng là small-file problem cũ được dựng lại dưới tên khác. `order by customer_name` để các hàng cùng khách hàng nằm liền nhau: file 08-09 sau khi nén có 5 row group, 3.500 hàng của ACME gom vào 2 group đầu (group 0 có `min=max='ACME'`), và 2 group kế tiếp có `min > 'ACME'` nên engine loại được — thay vì layout gốc rải đều khiến mọi group đều có `min='ACME', max='Cust_0650'` và không group nào loại được. `row_group_size = 2048` vì một ngày chỉ ~9.340 hàng, mặc định 122.880 gói cả ngày vào **một** row group mà min/max của nó phủ hết 650 khách hàng — vô dụng dù đã sắp xếp.

Tuy vậy tôi đã đo đối chứng bốn biến thể (có/không `ORDER BY` × `row_group_size` mặc định/2048/1000) và phải ghi nhận cho đúng: **`rows scanned` = 9.324 ở cả bốn**, và cũng bằng 9.324 với mọi ngày được filter kể cả ngày có 9.409 hàng. Nghĩa là metric `OPERATOR_ROWS_SCANNED` mà bài này chấm scale theo **số file được mở**, không theo số row group được decode. Vậy toàn bộ 536× đến từ **partition pruning (14 file → 1 file) cộng với predicate sargable**; `ORDER BY` và `row_group_size` là layout đúng cho dữ liệu lớn hơn và cho tỷ lệ nén (20 MB → 3,8 MB là lợi ích đo được của chúng), nhưng **không** phải nguyên nhân của con số trong bảng chấm. Ghi chú kỹ thuật thêm: `COPY … PARTITION_BY` không bảo toàn trọn vẹn thứ tự `ORDER BY` trong từng file — hàng được đệm và flush theo partition, nên file 08-09 có 4 row group sắp đúng thứ tự (`ACME` → `Cust_0520`) và 1 group "vét" cuối cùng trải rộng `Cust_0002` → `Cust_0650`, không loại được.

### Bài B — Consumer gặp sự cố giữa batch

| | |
|---|---|
| **Triệu chứng** | `make crash-test`: chạy một mạch được 20.000 hàng; bị `kill -9` ở lô 7 rồi khởi động lại chỉ còn **19.500 hàng — mất đúng 500 hàng**, tức trọn một lô, và không có hàng nào trùng. |
| **Nguyên nhân** | Thứ tự thao tác trong `consume()` là `commit() → write_batch()`, tức **at-most-once**: offset được commit **trước** khi dữ liệu nằm trên đĩa. Offset là lời khẳng định "mọi message tới đây đã xử lý xong" — commit trước là hứa trước khi làm. Tiến trình chết ở giữa hai bước: lô 7 chưa được ghi nhưng offset đã dịch lên 3.500, nên lần khởi động lại đọc từ 3.500 và **bỏ qua vĩnh viễn** 500 message đó. Không có lỗi nào được ném ra, không hàng nào trùng để lộ dấu vết — chỉ đơn giản là thiếu. Cơ chế cần nhớ: **exactly-once không tồn tại ở tầng giao vận; hai thứ chọn được là mất dữ liệu (commit trước) hoặc trùng dữ liệu (commit sau), nên lựa chọn đúng là at-least-once cộng với một phép ghi idempotent — chuyển bài toán từ "đừng bao giờ phát lại" sang "phát lại bao nhiêu lần cũng vô hại".** |
| **Cách khắc phục** | `ingest/consumer.py` — **(a)** đảo thứ tự thành `write_batch() → commit()`, bọc phép ghi trong một transaction để `kill -9` giữa lô không để lại lô nửa vời. **(b)** `DDL`: `event_id varchar primary key` (điều kiện để DuckDB chấp nhận `ON CONFLICT`); `write_batch()` đổi từ `INSERT` thuần thành `insert … on conflict (event_id) do update set …`. |
| **Bằng chứng** | trước: 19.500 / 20.000 hàng, **mất 500** · sau: **20.000 hàng / 20.000 event_id khác nhau**, `không mất bản ghi ✓`, `không trùng bản ghi ✓`, `C == A ✓` → **BÀI MỞ RỘNG B: ĐẠT**. Bằng chứng cho thấy at-least-once đã hoạt động đúng như thiết kế: offset commit được dừng ở 3.000 (không phải 3.500), lượt khởi động lại **ghi 17.000 message** = 500 phát lại + 16.500 mới, nhưng bảng chỉ có 20.000 hàng — upsert đã hấp thụ đúng 500 hàng phát lại đó. |

**`DO UPDATE` khác `DO NOTHING` ở đâu khi một message được phát lại với nội dung đã đổi? Chọn cái nào?**

> Với message phát lại **y nguyên**, hai lựa chọn cho kết quả giống nhau — và đó là trường hợp thường gặp nhất, nên rất dễ tưởng chúng tương đương. Chúng chỉ khác nhau khi cùng một `event_id` được gửi lại với **nội dung đã đổi** (upstream sửa rồi phát lại, hoặc một retry mang payload đã được làm giàu thêm): `DO UPDATE` hội tụ về phiên bản mới nhất, còn `DO NOTHING` đóng băng vĩnh viễn phiên bản đến trước và **âm thầm bỏ bản sửa** — không lỗi, không log, y hệt kiểu mất dữ liệu im lặng của `try_cast` ở nhiệm vụ 3.
>
> Tôi chọn `DO UPDATE`, cùng một lý lẽ với `merge` ở nhiệm vụ 1: bảng đích mô tả **trạng thái mới nhất của một khoá**, nên phép ghi phải là "đặt trạng thái", không phải "thêm một lần xuất hiện". `DO NOTHING` chỉ đúng khi khoá là bất biến theo định nghĩa (ví dụ log append-only mà mỗi `event_id` là một sự kiện đã xảy ra, không bao giờ được sửa) — và ngay cả lúc đó thì `DO UPDATE` cũng cho cùng kết quả, nên nó là lựa chọn mặc định an toàn hơn.

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Chạy pipeline **hai lần liền** rồi so số hàng — đây là phép thử rẻ nhất và nó phát hiện được cả một lớp lỗi mà không log nào báo. Sau đó với mỗi model `incremental`, đọc `config()` và tự hỏi: khoá của grain này là gì, và dbt đang generate `INSERT` hay `MERGE`? Cách chắc chắn là đọc `dbt/target/run/.../<model>.sql` — câu lệnh thật gửi xuống database — thay vì đoán từ `config()`. |
| 2 | Với mọi filter `incremental`, hỏi hai câu: **watermark đặt trên đại lượng nào**, và **dữ liệu thực tế đến theo đại lượng nào**? Nếu hai đại lượng đó khác nhau thì phải đo phân bố độ trễ giữa chúng (p50/p95/p99/max) trước khi tin vào con số nào. Và luôn kiểm tra tính đúng **sâu hơn số hàng** — ở lab này số hàng chỉ lộ 455 hàng thiếu, trong khi 5.746 cặp khác đang sai số liệu một cách im lặng. |
| 3 | Tìm mọi phép chuyển kiểu "an toàn" (`try_cast`, `safe_cast`, `coalesce` bọc quanh parse) và hỏi: **phần thất bại đi đâu?** Nếu câu trả lời là "thành NULL" mà không có bảng quarantine nào nhận, đó là một điểm mất dữ liệu im lặng đang chờ nguồn đổi format. Kèm theo: kiểm tra `contract` đang bật hay tắt, và nhớ rằng contract chỉ ràng buộc **kiểu** — miền giá trị phải có test riêng, vì `priority = 99` vẫn đúng là integer. |

**Một quan sát chung.** Ba sự cố có bề ngoài khác nhau, nhưng cùng một nguyên nhân gốc: **phép ghi không idempotent**, ở ba dạng khác nhau. Nhiệm vụ 1 là `INSERT` thay vì `MERGE`. Nhiệm vụ 2 cũng vậy, chỉ khác là nó bị lỗi thiếu dữ liệu che mất. Bài mở rộng B là `INSERT` thay vì `UPSERT` ở tầng consumer. Nhiệm vụ 3 là một biến thể: `try_cast` biến dữ liệu không hợp lệ thành NULL mà không ai nhận, tức phép ghi *mất* thông tin thay vì *nhân bản* nó. Và điểm chung đáng nhớ nhất: **không sự cố nào trong bốn sự cố này làm pipeline đỏ.** `dbt test` pass 9/9 suốt thời gian đó. Thứ phát hiện ra chúng là chạy lại hai lần và so checksum, chứ không phải chờ báo lỗi.

### Bảng tự chấm

| | Của tôi | Kỳ vọng | ✓/✗ |
|---|---|---|---|
| `gold_training_set` — số hàng | 12.480 | 12.480 | ✓ |
| `gold_training_set` — ổn định 3 lượt | `8dd7c98653` ×3 | ✓ | ✓ |
| `gold_feature_daily` — số hàng | 9.100 | 9.100 | ✓ |
| `gold_feature_daily` — ổn định 3 lượt | `f8d3f591f0` ×3 | ✓ | ✓ |
| `gold_doc_chunks` — số hàng | 31.200 | 31.200 | ✓ |
| `quarantine_tickets` — số hàng | 312 | 312 | ✓ |
| `silver_tickets` — số ticket | 12.480 | 12.480 | ✓ |
| `dbt test` | 11/11 pass | pass, > 9 test | ✓ |
| P99 độ trễ đo được | **2,726 ngày** | (ghi số) | ✓ |
| **Tổng verify** | 4/4 | 4/4 tiêu chí | ✓ |
| *(thưởng)* Bài A — `rows scanned` | 5.000.000 → 9.324 (536,3×) | ≥ 10× | ✓ |
| *(thưởng)* Bài B — `make crash-test` | ĐẠT | ĐẠT | ✓ |

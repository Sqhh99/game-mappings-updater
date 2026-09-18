# 定期更新 Loop（抓取 → 映射 → 建库 → 发版）

本文整理 `game-mappings-updater` 从 FLiNG 抓取到发布 SQLite 数据库的可重复流程。  
目标是做成**可定期执行的 loop**；在自动化完全放权前，建议每个阶段都先人工确认。

参考一次成功跑通的版本：[v0.0.4](https://github.com/Sqhh99/game-mappings-updater/releases/tag/v0.0.4)。

## 总览

```text
scrape ──► 核对新增/移除
    │
    ▼
补齐 game_mappings_manual.json（zh / ja）──► 核对译名
    │
    ▼
bump 版本 + build-sqlite ──► sqlite-status 检查
    │
    ▼
PR → merge 到 main → tag → GitHub Release
    │
    ▼
编辑 Release 简介（必须含新增/移除与中日文对照表）
```

**翻译来源约定：** IGDB / Steam / Wikidata 翻译 CLI 已移除。游戏名中日文由外部流程（AI 或人工）写入 `output/game_mappings_manual.json`，本仓库负责抓取、建库与发版。

## 0. 前置条件

- Python ≥ 3.12，[uv](https://docs.astral.sh/uv/)
- `gh` 已登录，且具备 `repo`、`workflow` 等发版所需权限
- 从最新 `main` 开分支工作（推荐 `release/vX.Y.Z`）
- 当前包版本见 `pyproject.toml`（发版时递增，例如 `0.0.4` → `0.0.5`）

```bash
git checkout main
git pull origin main
git checkout -b release/vX.Y.Z
```

## 1. 抓取（scrape）

```bash
uv run game-mappings-updater scrape
```

产出（写入 `output/`）：

| 文件 | 说明 |
|------|------|
| `fling_all_trainers.json` | 完整修改器列表 |
| `fling_game_names.json` | 游戏名列表（去掉 Trainer 后缀） |

### 本阶段核对

对比上一正式版（或上一提交）的 `fling_game_names.json`：

- 新增游戏名列表与数量
- 移除游戏名列表与数量（常见于改名，例如 `Crimson Desert` → `Crimson Desert Enhanced`）
- 总数 before → after

**通过标准：** 操作者确认抓取 diff 合理后再进入映射阶段。

> 临时 diff 文件（如 `_scrape_diff_*.json`）仅供本地核对，**不要提交**。

## 2. 补齐翻译映射

对每个**新增且尚未出现在** `game_mappings_manual.json` 的英文名，追加：

```json
{ "en": "...", "zh": "...", "ja": "..." }
```

### 映射规则

1. **优先官方译名**（商店页、发行商新闻、系列既有译名）。
2. **找不到官方译名时意译**（中文、日文都要意译）。
3. **不要把英文原样当作日文占位**（除非官方中日文本身保留英文副标题，例如 `鬼武者 Way of the Sword`）。
4. **改名/替换要更新旧映射**：删除或改写旧 `en` 条目，避免 stale 重复（例如删掉 `Crimson Desert`，只保留 `Crimson Desert Enhanced`）。

### 推荐做法

**优先直接根据 scrape diff 编辑** `output/game_mappings_manual.json`（本阶段的主路径）。  
`export-missing` / `import-missing` **不会读取刚 scrape 出的 JSON**，它们查询的是现有的 `output/fling_translations.db`。因此若 scrape 后立刻导出/导入，新游戏不会出现在模板里，`import-missing` 还可能把它们判成 unknown / non-missing。

若要用这两条辅助命令，必须先用**本轮 JSON**临时重建一次 DB（可先不 bump 正式版本号）：

```bash
# 用本轮 scrape 结果刷新 SQLite，再导出缺失模板
uv run game-mappings-updater build-sqlite
uv run game-mappings-updater export-missing

# 编辑 output/game_mappings_missing.json 补齐 zh / ja 后：
uv run game-mappings-updater import-missing --check-only
uv run game-mappings-updater import-missing
```

也可以一次跑 `uv run game-mappings-updater update`（内部顺序是 scrape → build-sqlite → export-missing），再编辑缺失模板并 `import-missing`。

### 本阶段核对

向操作者展示拟写入的对照表：

| English | 简体中文 | 日本語 |
|---------|----------|--------|
| ... | ... | ... |

**通过标准：** 操作者确认译名（或给出修改）后再建库。

## 3. 重建 SQLite

1. 递增版本：`pyproject.toml` 与 `uv.lock` 中的包版本（例如 `0.0.4` → `0.0.5`）。
2. 建库并写入预览用的 release tag：

```bash
uv run game-mappings-updater build-sqlite --release-tag vX.Y.Z
uv run game-mappings-updater sqlite-status --limit 20
```

### 本阶段核对

关注 `sqlite-status`：

- `Missing Manual` / `Missing Chinese` / `Missing Japanese`：对本次抓取结果理想为 **0**
- `Release Tag` 与目标版本一致
- 抽查改名条目与新增条目是否在库中

**通过标准：** 状态可接受后再提交发版 PR。

### 应提交的文件

```text
output/fling_all_trainers.json
output/fling_game_names.json
output/game_mappings_manual.json
output/fling_translations.db
pyproject.toml
uv.lock
```

### 不要提交

- `output/game_mappings_missing.json`（gitignore）
- `output/trainer_covers/`（gitignore）
- 本地临时文件：`output/_*.json` 等

## 4. PR → merge → tag → Release

### 4.1 提交并开 PR

```bash
git add output/fling_all_trainers.json \
        output/fling_game_names.json \
        output/game_mappings_manual.json \
        output/fling_translations.db \
        pyproject.toml uv.lock
git commit -m "Release vX.Y.Z: refresh FLiNG scrape and translation DB"
git push -u origin release/vX.Y.Z
gh pr create --base main --head release/vX.Y.Z \
  --title "Release vX.Y.Z" \
  --body "..."
```

### 4.2 合并后再打 tag（顺序很重要）

```bash
gh pr merge <N> --merge --delete-branch
git checkout main
git pull origin main
git tag vX.Y.Z
git push origin vX.Y.Z
```

推送 tag 会触发 `.github/workflows/make-release.yml`：

- 校验 tag 提交里存在 `output/fling_translations.db`
- 将 DB 内 `metadata.release_tag` 盖章为当前 tag 并校验
- 创建 GitHub Release，并上传 `fling_translations.db` 作为 asset

**注意：** CI **不会**重新 scrape / 重建数据库；发上去的就是提交里的那份 DB。

### 4.3 编辑 Release 简介（必做）

workflow 默认 body 只有一句模板，**不够用**。发版后立刻用完整简介覆盖：

```bash
gh release edit vX.Y.Z --notes-file /path/to/notes.md
```

简介至少包含：

1. **Summary**：抓取数量变化（before → after）、+/- 数量、建库结果摘要  
2. **Removed trainers**：移除项及原因（如被 Enhanced 替代）  
3. **New trainers & translation mappings**：完整表格  

   `| English | 简体中文 | 日本語 |`

4. **Asset**：说明附件 `fling_translations.db`

示例结构可参考 [v0.0.4 Release notes](https://github.com/Sqhh99/game-mappings-updater/releases/tag/v0.0.4)。

### 4.4 验收

- [ ] Release 页面已发布且非 draft  
- [ ] 附件 `fling_translations.db` 可下载  
- [ ] 简介含新增/移除与中日文对照表  
- [ ] tag 指向**已包含新 DB 的 merge commit**（不是旧 main）

## 5. 常见坑

| 问题 | 处理 |
|------|------|
| 在 PR 合并前就给 `main` 打 tag | 会发布旧 DB。删除远程 tag 与错误 Release，合并正确 PR 后再重新打 tag 并 push |
| Release 简介只有一句模板 | `gh release edit` 补上对照表 |
| 日文仍是英文占位 | 按规则意译后重建 DB 再发版 |
| 改名后旧 `en` 仍留在 manual | 删除或改写旧条目，避免双份 |
| 本地 `--release-tag` 与最终 tag 不一致 | 合并前改对；CI 也会按 push 的 tag 再盖章一次 |
| scrape 后立刻 `export-missing` / `import-missing` | 这两条读的是旧 DB。先 `build-sqlite`（或跑 `update`）再用，或直接改 `game_mappings_manual.json` |

## 6. 建议节奏与自动化

| 阶段 | 建议 |
|------|------|
| 抓取 | 每周或每两周跑一次 `scrape`，先出 diff |
| 映射 | AI 批量拟译 + 人工快速过目（尤其冷门意译） |
| 建库/发版 | 有新增（或重要改名）再发；无变化可跳过 |
| 全自动 | 等「每阶段确认」磨合稳定后，再把本 loop 做成定时 routine / CI；自动发版仍建议保留 Release notes 生成步骤 |

### 未来自动化可调用的最小步骤

1. `scrape` → 若 `added`/`removed` 为空则结束并静默  
2. 为 `added` 生成 zh/ja → 写入 manual（改名则更新旧条）  
3. bump 版本 → `build-sqlite --release-tag` → `sqlite-status` 门禁  
4. PR / merge / tag / `gh release edit` 写入对照表  

在门禁与 notes 生成可靠之前，保持人工确认闸门。

## 7. 相关文档与命令速查

- 日常命令与 SQLite 字段说明：见 [README.md](./README.md)
- 发版 workflow：`.github/workflows/make-release.yml`

```bash
uv run game-mappings-updater scrape
uv run game-mappings-updater update          # scrape → build-sqlite → export-missing（顺序正确）
uv run game-mappings-updater build-sqlite --release-tag vX.Y.Z
uv run game-mappings-updater sqlite-status
# export/import-missing 依赖当前 fling_translations.db；scrape 后须先 build-sqlite
uv run game-mappings-updater export-missing
uv run game-mappings-updater import-missing
```

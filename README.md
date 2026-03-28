# game-mappings-updater

从 [flingtrainer.com](https://flingtrainer.com/) 爬取所有修改器（Trainer）名称，并通过 [IGDB API](https://api-docs.igdb.com/)、Steam 商店接口和 Wikidata API 获取官方中文/日文译名。

## 环境要求

- Python ≥ 3.12
- [uv](https://docs.astral.sh/uv/) 包管理器

## 配置

复制 `.env.example` 为 `.env`，填入你的 Twitch/IGDB 凭据：

```bash
cp .env.example .env
```

```env
IGDB_CLIENT_ID=your_client_id
IGDB_CLIENT_SECRET=your_client_secret
```

> Steam 翻译功能不需要额外配置；只有 IGDB 翻译需要这些凭据。
>
> 凭据在 [Twitch Developer Console](https://dev.twitch.tv/console) 注册应用获取。

## 使用方法

```bash
# 1. 爬取所有修改器名称
uv run game-mappings-updater scrape

# 2. 一键刷新抓取结果、重建 SQLite，并导出缺失映射模板
uv run game-mappings-updater update

# 3. 通过 IGDB 翻译游戏名（需先运行 scrape）
uv run game-mappings-updater translate

# 4. 通过 Steam 商店接口翻译游戏名（需先运行 scrape）
uv run game-mappings-updater translate-steam

# 5. 通过 Wikidata API 翻译游戏名（需先运行 scrape）
uv run game-mappings-updater translate-wikidata

# 6. 并发执行三个翻译源
uv run game-mappings-updater translate-all

# 7. 指定并发 worker 数
uv run game-mappings-updater translate-all --workers 3

# 8. 将 manual 映射和 FLiNG 抓取结果汇总为 SQLite 数据库
uv run game-mappings-updater build-sqlite

# 9. 查看 SQLite 数据库状态
uv run game-mappings-updater sqlite-status

# 10. 限制显示的待处理记录条数
uv run game-mappings-updater sqlite-status --limit 50

# 11. 单独导出缺失映射模板
uv run game-mappings-updater export-missing

# 12. 只校验补齐后的 JSON 格式和内容
uv run game-mappings-updater import-missing --check-only

# 13. 导入补齐后的缺失映射，并自动重建 DB / 重新导出缺失模板
uv run game-mappings-updater import-missing
```

`translate`、`translate-steam` 和 `translate-wikidata` 都支持增量运行：已翻译的游戏会自动跳过，中断后重新运行会继续。
`translate-all` 会并发运行这三个翻译源，并把每个来源的输出分别写入对应的 JSON 文件。

## 输出文件

运行后在 `output/` 目录生成：

| 文件 | 说明 |
|------|------|
| `fling_all_trainers.json` | 完整修改器列表（名称、URL、来源） |
| `fling_game_names.json` | 游戏名列表（去掉 "Trainer" 后缀） |
| `fling_translations_igdb.json` | IGDB 翻译结果（英文名 + 中文简体/繁体 + 日文） |
| `fling_translations_steam.json` | Steam 翻译结果（英文名 + 中文简体 + 日文 + Steam 调试字段） |
| `fling_translations_wikidata.json` | Wikidata 翻译结果（英文名 + 中文简体 + 日文 + Wikidata 调试字段） |
| `game_mappings_manual.json` | 手工维护的英文/简中/日文映射，作为 SQLite 导出的唯一翻译来源 |
| `game_mappings_missing.json` | 当前缺失翻译映射的导出模板，补齐后可直接导入 |
| `fling_translations.db` | 基于 manual 映射和 FLiNG 抓取结果生成的 SQLite 数据库，供搜索系统直接使用 |

## SQLite 结构

`build-sqlite` 会基于当前 `output/` 里的这三份文件生成 `fling_translations.db`：

- `game_mappings_manual.json`
- `fling_game_names.json`
- `fling_all_trainers.json`

其中 `game_mappings_manual.json` 是唯一翻译真源；SQLite 不再依赖 `fling_translations_igdb.json`、`fling_translations_steam.json` 或 `fling_translations_wikidata.json`。

数据库现在包含：

- `games`: 主表。每个英文游戏名一行，包含简体中文、日文、trainer 信息、`status`、缺失标记、`first_seen_at`、`last_seen_at`
- `game_aliases`: 搜索专用别名表，只保留英文名、手工简体中文、手工日文，以及规范化后的 `normalized_alias`
- `metadata`: 构建时间、输入文件数量、手工映射去重/冲突统计
- `db_status`: 汇总视图，用于查询数据库当前状态，例如缺失映射数量、缺简中/日文数量、manual-only 数量
- `needs_review`: 明细视图，列出 `status != 'ok'` 或手工映射冲突的游戏

命令行里也可以直接查看这些状态：

```bash
uv run game-mappings-updater sqlite-status
```

缺失映射维护流程：

```bash
uv run game-mappings-updater update
# 编辑 output/game_mappings_missing.json
uv run game-mappings-updater import-missing --check-only
uv run game-mappings-updater import-missing
```

`games.status` 的主状态值固定为：

- `ok`
- `missing_manual_mapping`
- `missing_chinese`
- `missing_japanese`
- `missing_translations`
- `manual_only`

`build-sqlite` 支持增量更新语义：每次会根据最新 JSON 重建数据库快照，但会继承旧库里已存在游戏的 `first_seen_at`。这样你后续只需要：

1. 重新执行 `scrape`
2. 给新游戏补 `game_mappings_manual.json`
3. 再执行 `build-sqlite`

新增的 FLiNG 游戏会自动进入数据库；还没补手工映射的会标记为 `missing_manual_mapping`，方便后续维护。

`game_mappings_missing.json` 的每一项都包含：

- `en`
- `zh`
- `ja`
- `status`
- `trainer_name`
- `trainer_url`

导入时只会读取 `en / zh / ja`，其余字段只用于人工排查。`import-missing` 默认只补缺失，不覆盖 `game_mappings_manual.json` 里已有的非空值。

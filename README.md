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

# 2. 通过 IGDB 翻译游戏名（需先运行 scrape）
uv run game-mappings-updater translate

# 3. 通过 Steam 商店接口翻译游戏名（需先运行 scrape）
uv run game-mappings-updater translate-steam

# 4. 通过 Wikidata API 翻译游戏名（需先运行 scrape）
uv run game-mappings-updater translate-wikidata
```

`translate`、`translate-steam` 和 `translate-wikidata` 都支持增量运行：已翻译的游戏会自动跳过，中断后重新运行会继续。

## 输出文件

运行后在 `output/` 目录生成：

| 文件 | 说明 |
|------|------|
| `fling_all_trainers.json` | 完整修改器列表（名称、URL、来源） |
| `fling_game_names.json` | 游戏名列表（去掉 "Trainer" 后缀） |
| `fling_translations_igdb.json` | IGDB 翻译结果（英文名 + 中文简体/繁体 + 日文） |
| `fling_translations_steam.json` | Steam 翻译结果（英文名 + 中文简体 + 日文 + Steam 调试字段） |
| `fling_translations_wikidata.json` | Wikidata 翻译结果（英文名 + 中文简体 + 日文 + Wikidata 调试字段） |

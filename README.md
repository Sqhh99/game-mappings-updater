# game-mappings-updater

从 [flingtrainer.com](https://flingtrainer.com/) 爬取所有修改器（Trainer）名称，用于维护 FLiNG Downloader 的游戏名映射表。

## 功能

- 爬取**现代修改器**（2019.05 之后发布），来源：[All Trainers (A-Z)](https://flingtrainer.com/all-trainers/)
- 爬取**归档修改器**（2012 – 2019.05），来源：[My Trainers Archive](https://flingtrainer.com/trainer/my-trainers-archive/)
- 自动去重、提取纯游戏名（去除 "Trainer" 后缀）

## 环境要求

- Python ≥ 3.12
- [uv](https://docs.astral.sh/uv/) 包管理器

## 使用方法

```bash
# 爬取所有修改器名称
uv run game-mappings-updater scrape
```

## 输出文件

运行后在 `output/` 目录生成：

| 文件 | 说明 |
|------|------|
| `fling_all_trainers.json` | 完整列表（名称、URL、来源标记） |
| `fling_game_names.json` | 仅游戏名（已去除 "Trainer" 后缀） |

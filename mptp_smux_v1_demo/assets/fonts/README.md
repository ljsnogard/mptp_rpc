# 字体资产

- `MapleMono-NF-CN-Regular.ttf`：Maple Mono NF CN 8.001 的 Regular 字重，来自
  `MapleMono-NF-CN-unhinted.zip`；许可证为 SIL OFL 1.1，见 [`OFL.txt`](OFL.txt)。
- 用途：`bevy_egui` 默认字体不含 CJK，客户端启动时由
  [`src/bin/scene-client/font_.rs`](../../src/bin/scene-client/font_.rs) 把它作为最低优先级
  回退字体加入 `Proportional` / `Monospace`，避免界面中文显示成方块。
- 替换字体时保持文件名不变即可；找不到该文件时客户端会继续尝试可执行文件旁的
  `assets/fonts/` 目录与系统 CJK 字体。

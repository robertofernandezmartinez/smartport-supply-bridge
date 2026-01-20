# SmartPort Supply Chain Bridge 🚢📦

This project integrates maritime logistics data with warehouse inventory predictions. It serves as an automated decision-support system for supply chain managers.

## 🔗 Project Integration
This repository acts as the central intelligence hub for:
1. **[SmartPort AI Risk Warning](https://github.com/robertofernandezmartinez/smartport-ai-risk-early-warning):** Real-time maritime delay predictions and alerts.
2. **[Retail Stockout Risk Scoring](https://github.com/robertofernandezmartinez/retail-stockout-risk-scoring):** Inventory depletion forecasts.


## 🧠 The Intelligence Logic
1. **Maritime Input:** Monitors vessel delays and risk scores from the SmartPort AI project.
2. **Bridge Mapping:** Automatically links incoming vessels to specific product categories (e.g., Electronics, Toys).
3. **Inventory Impact:** Cross-references impacted categories with stockout predictions (14-day forecast).
4. **AI Notification:** If a delay risks a stockout, **GPT-4o-mini** generates a professional executive alert sent via **Telegram**.

## 🛠️ Tech Stack
- **Python:** Data orchestration with Pandas.
- **Google Sheets API:** Real-time data source.
- **OpenAI API:** Generative AI for logistics insights.
- **Telegram Bot API:** Automated communication channel.
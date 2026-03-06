# DMAI API Key Harvester

Part of the DMAI (David's Modular AI) System. Automatically discovers and validates API keys for AI services to feed the evolution engine.

## Services

- **Harvester**: 24/7 continuous scraping from GitHub, Pastebin, etc.
- **Validator**: Hourly validation of discovered keys
- **Redis**: Deduplication cache
- **PostgreSQL**: Key metadata storage
- **Persistent Disk**: Encrypted key storage

## API Patterns Supported

- OpenAI (sk-...)
- Anthropic Claude (sk-ant-...)
- Google Gemini (AIza...)
- Groq (gsk_...)
- DeepSeek (sk-...)
- Mistral
- Cohere
- Hugging Face
- Replicate

## Deployment

Deploy on Render using blueprint:

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy)

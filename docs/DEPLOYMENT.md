
# UK Insolvency Agent - Streamlit App Deployment

## Quick Start
1. Install dependencies: `pip install -r requirements.txt`
2. Set up Google Drive credentials (see Drive Setup below)
3. Run: `streamlit run app.py`

## File Structure
```
project/
├── app.py                    # Main Streamlit application
├── wc_insolvency_core.py     # Core backend functionality
├── wc_insolvency_cached.py   # Cached pipeline implementation
├── drive_client.py           # Google Drive integration
├── requirements.txt          # Python dependencies
├── secrets/
│   └── client_secret.json    # Google OAuth2 credentials
└── cache/                    # Cache directory (auto-created)
```

## Google Drive Setup
1. Go to Google Cloud Console
2. Create new project or select existing one
3. Enable Google Drive API
4. Create OAuth2 credentials (Desktop application)
5. Download client_secret.json to secrets/ folder
6. First run will prompt for authorization

## AWS Deployment
1. Create EC2 instance (t3.medium recommended)
2. Install Python 3.8+
3. Clone repository and install dependencies
4. Configure security group for port 8501
5. Run with: `streamlit run app.py --server.port 8501 --server.address 0.0.0.0`

## Environment Variables
- STREAMLIT_SERVER_PORT=8501
- STREAMLIT_SERVER_ADDRESS=0.0.0.0
- GOOGLE_APPLICATION_CREDENTIALS=secrets/client_secret.json

## Features
- Real-time job monitoring
- Intelligent caching system
- Google Drive integration
- Job history and analytics
- Mobile-responsive design
- Production-ready error handling

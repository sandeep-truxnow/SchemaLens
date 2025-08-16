# SchemaLens Setup Guide

## Prerequisites

### 1. Install AWS Session Manager Plugin

**macOS:**
```bash
curl "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/mac/sessionmanager-bundle.zip" -o "sessionmanager-bundle.zip"
unzip sessionmanager-bundle.zip
sudo ./sessionmanager-bundle/install -i /usr/local/sessionmanagerplugin -b /usr/local/bin/session-manager-plugin
```

**Linux:**
```bash
curl "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/linux_64bit/session-manager-plugin.rpm" -o "session-manager-plugin.rpm"
sudo yum install -y session-manager-plugin.rpm
```

**Windows:**
Download and install from: https://s3.amazonaws.com/session-manager-downloads/plugin/latest/windows/SessionManagerPluginSetup.exe

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure AWS CLI
```bash
aws configure
# Enter your AWS credentials
```

## Running the Application

### Local Execution (Recommended)
```bash
streamlit run aws.py
```

### Cloud Deployment Note
This application requires AWS SSM tunneling for secure database access. Cloud platforms like Streamlit Cloud don't support the Session Manager plugin, so local execution is required.

## Troubleshooting

**Error: "SessionManagerPlugin is not found"**
- Install the AWS Session Manager plugin (see step 1 above)
- Verify installation: `session-manager-plugin --version`

**Error: "Direct RDS connection failed"**
- Your RDS instance is private (security best practice)
- Use local execution with SSM tunneling
- Ensure your AWS credentials have SSM permissions
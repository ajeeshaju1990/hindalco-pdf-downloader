# Hindalco PDF Downloader - GitHub Actions

This repository automatically downloads the latest Hindalco Aluminium Price PDFs daily using GitHub Actions.

## Setup Instructions

### Step 1: Create a New GitHub Repository
1. Go to [GitHub](https://github.com) and sign in
2. Click the "+" icon in the top right corner
3. Select "New repository"
4. Name it something like "hindalco-pdf-downloader"
5. Make sure it's set to **Public** (required for free GitHub Actions)
6. Check "Add a README file"
7. Click "Create repository"

### Step 2: Add Files to Your Repository
1. In your new repository, click "Add file" → "Create new file"
2. Create the folder structure `.github/workflows/` first
3. Create these files one by one:

#### File 1: `.github/workflows/hindalco_downloader.py`
Copy the Python script from the first artifact above.

#### File 2: `.github/workflows/schedule.yml`
Copy the YAML workflow from the second artifact above.

#### File 3: `.github/workflows/requirements.txt`
Copy the requirements from the third artifact above.

#### File 4: `.github/workflows/README.md`
Copy this README content to the workflows folder as well.

### Step 3: Enable GitHub Actions
1. Go to your repository
2. Click on the "Actions" tab
3. If prompted, click "I understand my workflows, go ahead and enable them"

### Step 4: Test the Setup
1. Go to the "Actions" tab in your repository
2. Click on "Daily Hindalco PDF Download" workflow
3. Click "Run workflow" button to test it manually
4. Wait for it to complete (should take 2-3 minutes)

### Step 5: Check Results
- Downloaded PDFs will appear in the `hindalco_pdfs/` folder
- The log file `latest_hindalco_pdf.json` tracks the last downloaded PDF
- The workflow runs automatically every day at 9:00 AM IST

## File Structure
```
your-repo/
├── .github/
│   └── workflows/
│       ├── schedule.yml
│       ├── hindalco_downloader.py
│       ├── requirements.txt
│       └── README.md
├── hindalco_pdfs/
│   └── (downloaded PDFs will appear here)
├── latest_hindalco_pdf.json
└── README.md
```

## Key Changes Made for GitHub Actions

1. **Chrome instead of Firefox**: GitHub Actions works better with Chrome
2. **Relative paths**: No more hardcoded Windows paths
3. **Timestamped filenames**: PDFs are saved with timestamps
4. **JSON logging**: Better tracking of downloaded files
5. **Error handling**: More robust error handling
6. **Auto-commit**: Files are automatically committed to the repository

## Schedule
The script runs every day at 9:00 AM IST (3:30 AM UTC). You can change this in the workflow file by modifying the cron expression.

## Manual Trigger
You can also run the workflow manually:
1. Go to Actions tab
2. Select "Daily Hindalco PDF Download"
3. Click "Run workflow"

## Troubleshooting
- Check the Actions tab for any error logs
- Make sure your repository is public (private repos have limited free Actions minutes)
- The first run might take longer as it sets up the environment

## Note
All downloaded files will be stored in your GitHub repository, so you can access them from anywhere!

# Git Instructions - Pushing to GitHub

Step-by-step guide to push your e-commerce database project to GitHub.

---

## Table of Contents
1. [Quick Start](#quick-start)
2. [Detailed Steps](#detailed-steps)
3. [Common Git Commands](#common-git-commands)
4. [Troubleshooting](#troubleshooting)

---

## Quick Start

If you already have a GitHub repository set up:

```bash
# Navigate to project folder
cd path/to/ecommerce-database

# Add all files
git add .

# Commit changes
git commit -m "Add e-commerce database schema and business queries"

# Push to GitHub
git push origin main
```

---

## Detailed Steps

### Step 1: Set Up Git Configuration

```bash
# Set your name (one-time setup)
git config --global user.name "Your Full Name"

# Set your email (one-time setup)
git config --global user.email "your.email@example.com"

# Verify configuration
git config --list
```

### Step 2: Create GitHub Repository

1. Go to https://github.com/new
2. Fill in repository details:
   - **Repository name:** `ecommerce-database`
   - **Description:** `E-Commerce Database with Advanced SQL Queries and Business Analytics`
   - **Visibility:** Select "Public" (for portfolio) or "Private"
   - **Initialize this repository with:**
     - ✅ Skip this - you already have files
3. Click **"Create repository"**
4. Copy the repository URL (HTTPS or SSH)

### Step 3: Initialize Git in Your Project

Navigate to your project folder:

```bash
cd /path/to/ecommerce-database
```

Initialize git (only if you haven't already):

```bash
# Initialize git repository
git init

# Verify initialization
ls -la  # You should see a .git folder
```

### Step 4: Add All Files

```bash
# Add all files to staging area
git add .

# Or add specific files
git add README.md
git add schema.sql
git add queries.sql
git add .gitignore
git add SETUP.md

# Check status
git status
```

### Step 5: Create Initial Commit

```bash
# Create a commit with descriptive message
git commit -m "Initial commit: E-commerce database schema and 20 business queries

- 9 interconnected database tables
- 20 advanced SQL queries for business analytics
- ER diagram and documentation
- Complete setup and installation guide"
```

### Step 6: Connect to GitHub Repository

```bash
# Add remote repository (replace with your GitHub URL)
git remote add origin https://github.com/yourusername/ecommerce-database.git

# Verify remote was added
git remote -v
```

### Step 7: Rename Branch to main (if needed)

```bash
# Rename branch to main
git branch -M main

# Verify branch name
git branch
```

### Step 8: Push to GitHub

```bash
# Push to GitHub (first time with -u flag)
git push -u origin main

# Subsequent pushes
git push
```

### Step 9: Verify on GitHub

1. Open https://github.com/yourusername/ecommerce-database
2. Verify all files are present:
   - [ ] README.md
   - [ ] schema.sql
   - [ ] queries.sql
   - [ ] SETUP.md
   - [ ] GIT_INSTRUCTIONS.md
   - [ ] .gitignore

---

## Common Git Commands

### Day-to-Day Workflow

```bash
# Check status
git status

# View commit history
git log

# View recent commits (one line each)
git log --oneline -10

# Add files
git add .

# Commit with message
git commit -m "Brief description of changes"

# Push to GitHub
git push

# Pull latest changes from GitHub
git pull
```

### Viewing Changes

```bash
# See what changed in a file
git diff schema.sql

# See what will be committed
git diff --cached

# See commits that will be pushed
git log origin/main..main
```

### Undo Changes

```bash
# Undo changes to a file (before adding)
git checkout schema.sql

# Unstage a file
git reset HEAD queries.sql

# Undo last commit (keep changes)
git reset --soft HEAD~1

# Undo last commit (discard changes)
git reset --hard HEAD~1
```

### Branching (Optional)

```bash
# Create new branch
git checkout -b feature/add-triggers

# Switch to branch
git checkout main

# List branches
git branch

# Delete branch
git branch -d feature/add-triggers

# Push branch to GitHub
git push origin feature/add-triggers
```

---

## Complete Example Workflow

Here's a complete example of working with your project:

```bash
# 1. Navigate to project
cd ~/ecommerce-database

# 2. Check status
git status

# 3. Make changes to files (edit, add new queries, etc.)
# ... edit files ...

# 4. Check what changed
git status
git diff queries.sql

# 5. Stage changes
git add .

# 6. Review staged changes
git status

# 7. Commit
git commit -m "Add new query for product recommendations"

# 8. Check log
git log --oneline -5

# 9. Push to GitHub
git push

# 10. Verify on GitHub
# Open https://github.com/yourusername/ecommerce-database
```

---

## Authentication Methods

### HTTPS (Easiest)

```bash
git remote add origin https://github.com/yourusername/ecommerce-database.git
git push -u origin main
# Enter GitHub username and password (or personal access token)
```

**Note:** GitHub deprecated password authentication. Use Personal Access Token instead:
1. Go to GitHub Settings → Developer settings → Personal access tokens
2. Generate new token with 'repo' scope
3. Use token as password

### SSH (More Secure)

#### Set Up SSH Key (One-time)

```bash
# Generate SSH key (if not already done)
ssh-keygen -t ed25519 -C "your.email@example.com"

# Follow prompts (press Enter for default location)
# When asked for passphrase, press Enter (or set one for security)

# Add key to SSH agent
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519

# Display public key
cat ~/.ssh/id_ed25519.pub
```

#### Add SSH Key to GitHub

1. Copy the output from `cat ~/.ssh/id_ed25519.pub`
2. Go to GitHub Settings → SSH and GPG keys
3. Click "New SSH key"
4. Paste the key
5. Add title and click "Add SSH key"

#### Use SSH with Git

```bash
# Change remote to SSH
git remote set-url origin git@github.com:yourusername/ecommerce-database.git

# Verify
git remote -v

# Push (no password needed)
git push -u origin main
```

---

## Troubleshooting

### Issue: "fatal: not a git repository"

**Solution:**
```bash
# Initialize git
git init

# Add remote
git remote add origin https://github.com/yourusername/ecommerce-database.git
```

### Issue: "fatal: 'origin' does not appear to be a 'git' repository"

**Solution:**
```bash
# Check remote
git remote -v

# Add missing remote
git remote add origin https://github.com/yourusername/ecommerce-database.git

# Or update existing remote
git remote set-url origin https://github.com/yourusername/ecommerce-database.git
```

### Issue: "Permission denied (publickey)"

**Solution:**
```bash
# Use HTTPS instead of SSH
git remote set-url origin https://github.com/yourusername/ecommerce-database.git

# Or set up SSH correctly (see SSH section above)
```

### Issue: "Everything up-to-date" but changes not visible on GitHub

**Solution:**
```bash
# Make sure you've committed
git status  # Should show nothing to commit

# Make sure you've pushed
git log origin/main..main  # Should be empty

# Force push (only if needed)
git push --force origin main
```

### Issue: "Your branch is ahead of 'origin/main' by X commits"

**Solution:**
```bash
# Just push
git push

# Or specify branch
git push origin main
```

### Issue: Merge conflicts

**Solution:**
```bash
# If pulling causes conflicts
git pull

# Edit conflicting files (search for <<<<<<, ======, >>>>>>)
# Fix conflicts manually

# Stage resolved files
git add .

# Commit merge
git commit -m "Resolve merge conflicts"

# Push
git push
```

---

## Best Practices

### Commit Messages

Write clear, descriptive commit messages:

```bash
# Good
git commit -m "Add top-selling products query and inventory alerts"

# Bad
git commit -m "fix"
```

### Commit Often

```bash
# Commit logical changes together
git commit -m "Add schema.sql with 9 interconnected tables"
git commit -m "Add 20 business analytics queries"
git commit -m "Add README and setup documentation"
```

### Push Regularly

```bash
# Push at end of day or when feature is complete
git push
```

### Always Pull Before Push

```bash
# Pull latest changes
git pull

# Make your changes
# ... edit files ...

# Push
git push
```

---

## GitHub Profile Enhancement

### Update Your GitHub Profile

1. Go to https://github.com/settings/profile
2. Add a profile picture
3. Add bio/description
4. Add your website/portfolio

### Create a Great Repository

1. **Add Topics:** Go to Settings → About → Topics
   - Add: `database`, `sql`, `postgresql`, `ecommerce`, `analytics`

2. **Add Star:** Add star icon (⭐) at top of repository

3. **Share:** Share the link on:
   - LinkedIn
   - Twitter
   - Portfolio
   - Resume

---

## Next Steps

1. ✅ Push your project to GitHub
2. ✅ Add topics and description
3. ✅ Share with others
4. ✅ Use for portfolio/interviews
5. ✅ Continue adding more queries and features

---

**Your GitHub URL:** https://github.com/yourusername/ecommerce-database

---

Good luck! Your database project will be a great addition to your portfolio! 🚀

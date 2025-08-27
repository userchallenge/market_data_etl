# Claude Code Installation Fix Guide

## Current Problem Summary

**Issue**: Multiple Claude Code installations causing version conflicts
- **Active Version**: 1.0.72 (old) at `/opt/homebrew/bin/claude`  
- **Available Version**: 1.0.90 (newer) at `/Users/cw/.local/bin/claude`
- **Root Cause**: Mixed installation methods (npm/homebrew + native binary)
- **Symptom**: `claude --version` shows 1.0.72 despite having 1.0.90 installed

## Progress Tracking

### Phase 1: Clean Up Mixed Installations
- [ ] 1.1 Remove native binary installation
- [ ] 1.2 Clean .zprofile PATH modifications
- [ ] 1.3 Remove npm global Claude (if exists)
- [ ] 1.4 Verify cleanup completed

### Phase 2: Standard Homebrew Reinstallation  
- [ ] 2.1 Update homebrew
- [ ] 2.2 Add/verify Anthropic tap
- [ ] 2.3 Reinstall Claude Code via homebrew
- [ ] 2.4 Verify installation location

### Phase 3: Shell Configuration
- [ ] 3.1 Clean up .zprofile
- [ ] 3.2 Check for .zshrc conflicts
- [ ] 3.3 Reload shell configuration
- [ ] 3.4 Verify PATH changes

### Phase 4: Final Verification
- [ ] 4.1 Test claude version
- [ ] 4.2 Test claude doctor
- [ ] 4.3 Test basic functionality
- [ ] 4.4 Document final state

---

## Phase 1: Clean Up Mixed Installations

### Step 1.1: Remove Native Binary Installation

**Command:**
```bash
# Check if native binary exists
ls -la ~/.local/bin/claude

# If it exists, remove it
rm -f ~/.local/bin/claude

# Remove the entire .local directory if it's empty (optional)
rmdir ~/.local/bin 2>/dev/null || true
rmdir ~/.local 2>/dev/null || true
```

**Expected Output:**
- First command should show file or "No such file" 
- After removal, file should not exist

### Step 1.2: Clean .zprofile PATH Modifications

**Command:**
```bash
# Backup current .zprofile
cp ~/.zprofile ~/.zprofile.backup

# Edit .zprofile to remove native Claude PATH
# Remove these lines if they exist:
# export PATH="/Users/cw/.local/bin:$PATH" 
# # Add native Claude to PATH
```

**Manual Edit Required:**
1. Run: `code ~/.zprofile` (or use your preferred editor)
2. Remove these lines if present:
   ```
   # Add native Claude to PATH
   export PATH="/Users/cw/.local/bin:$PATH"
   ```
3. Remove this line if present:
   ```
   # Native Claude now in PATH, no alias needed
   ```
4. Save and close

### Step 1.3: Remove NPM Global Claude (if exists)

**Commands:**
```bash
# Check if npm global Claude exists
npm list -g @anthropic-ai/claude-code 2>/dev/null || echo "No npm global Claude found"

# If it exists, uninstall it
npm uninstall -g @anthropic-ai/claude-code

# Verify removal
npm list -g @anthropic-ai/claude-code 2>/dev/null || echo "Successfully removed"
```

**Expected Output:**
- Should show "No npm global Claude found" or successful removal

### Step 1.4: Verify Cleanup Completed

**Commands:**
```bash
# Check what claude command points to now
which claude

# Check current version
claude --version

# Check PATH doesn't include .local/bin
echo $PATH | grep -o "/Users/cw/.local/bin" || echo "✅ .local/bin not in PATH"
```

**Expected Output:**
- `which claude` should still point to `/opt/homebrew/bin/claude`
- Version should still be 1.0.72 (will fix in next phase)
- PATH should not include `/Users/cw/.local/bin`

---

## Phase 2: Standard Homebrew Reinstallation

### Step 2.1: Update Homebrew

**Commands:**
```bash
# Update homebrew
brew update

# Check brew is working
brew --version
```

**Expected Output:**
- Should show latest homebrew version
- No error messages

### Step 2.2: Add/Verify Anthropic Tap

**Commands:**
```bash
# Check if tap exists
brew tap | grep anthropic-ai

# Add tap if missing
brew tap anthropic-ai/claude

# Verify tap was added
brew tap | grep anthropic-ai
```

**Expected Output:**
- Should show `anthropic-ai/claude` in tap list

### Step 2.3: Reinstall Claude Code via Homebrew

**Commands:**
```bash
# Uninstall current version first
brew uninstall claude-code 2>/dev/null || echo "No existing homebrew installation"

# Install latest version
brew install anthropic-ai/claude/claude-code

# Verify installation
brew list | grep claude
```

**Expected Output:**
- Should show successful installation
- `claude-code` should appear in brew list

### Step 2.4: Verify Installation Location

**Commands:**
```bash
# Check installation location
ls -la /opt/homebrew/bin/claude

# Check which claude is active
which claude

# Check new version
claude --version
```

**Expected Output:**
- File should exist at `/opt/homebrew/bin/claude`
- `which claude` should point to `/opt/homebrew/bin/claude`
- Version should be 1.0.90 or later

---

## Phase 3: Shell Configuration

### Step 3.1: Clean Up .zprofile

**Command:**
```bash
# View current .zprofile
cat ~/.zprofile
```

**Manual Verification:**
- Ensure NO lines adding `/Users/cw/.local/bin` to PATH
- Ensure homebrew shellenv line exists: `eval "$(/opt/homebrew/bin/brew shellenv)"`
- Keep your other environment variables (API keys, etc.)

**Expected .zprofile Content:**
```bash
# Set PATH, MANPATH, etc., for Homebrew.
eval "$(/opt/homebrew/bin/brew shellenv)"

# Environment variables
export OPENAI_API_KEY='your_key_here'
export GEMINI_API_KEY='your_key_here'  
export FRED_API_KEY='your_key_here'

# Auto-activate venv for Python projects
activate_python_venv() {
    # Your existing function
}

# Auto-activate on terminal start and directory changes
activate_python_venv

# Hook for directory changes (zsh)
if [[ -n "$ZSH_VERSION" ]]; then
    chpwd() { activate_python_venv; }
fi
```

### Step 3.2: Check for .zshrc Conflicts

**Commands:**
```bash
# Check if .zshrc exists
ls -la ~/.zshrc

# If it exists, check for PATH modifications
if [[ -f ~/.zshrc ]]; then
    echo "=== .zshrc content ==="
    cat ~/.zshrc
    echo "====================="
else
    echo "✅ No .zshrc file found"
fi
```

**Action Required:**
- If .zshrc exists and modifies PATH, it may override .zprofile
- If needed, move essential config from .zshrc to .zprofile
- Or ensure .zshrc doesn't conflict with homebrew PATH

### Step 3.3: Reload Shell Configuration

**Commands:**
```bash
# Reload .zprofile
source ~/.zprofile

# If .zshrc exists, reload it too
[[ -f ~/.zshrc ]] && source ~/.zshrc

# Start a new shell session to test
exec zsh
```

**Expected Behavior:**
- Commands should run without errors
- New shell should have updated PATH

### Step 3.4: Verify PATH Changes

**Commands:**
```bash
# Check current PATH
echo $PATH

# Verify homebrew paths are present and early in PATH
echo $PATH | grep -o "/opt/homebrew/bin" || echo "❌ Missing homebrew bin in PATH"

# Check which claude is active
which claude

# Verify it's the homebrew version
ls -la $(which claude)
```

**Expected Output:**
- PATH should include `/opt/homebrew/bin` near the beginning
- `which claude` should point to `/opt/homebrew/bin/claude`

---

## Phase 4: Final Verification

### Step 4.1: Test Claude Version

**Commands:**
```bash
# Check version
claude --version

# Should show 1.0.90 or later
```

**Expected Output:**
```
1.0.90 (Claude Code)
```

### Step 4.2: Test Claude Doctor

**Commands:**
```bash
# Run diagnostic (may need to press Enter to continue)
claude doctor
```

**Expected Output:**
- Should show single installation at `/opt/homebrew/bin/claude`
- Should show version 1.0.90+
- Should not show "Multiple installations found" warning

### Step 4.3: Test Basic Functionality

**Commands:**
```bash
# Test help command
claude --help

# Test in current directory (should work)
pwd
claude
```

**Expected Behavior:**
- Help should display properly
- Claude should start normally in interactive mode

### Step 4.4: Document Final State

**Commands to Save Final State:**
```bash
# Document final setup for future reference
echo "=== Final Claude Setup ===" > ~/claude_setup_final.txt
echo "Date: $(date)" >> ~/claude_setup_final.txt
echo "Version: $(claude --version)" >> ~/claude_setup_final.txt  
echo "Location: $(which claude)" >> ~/claude_setup_final.txt
echo "Installation method: Homebrew" >> ~/claude_setup_final.txt
echo "Shell config: .zprofile only" >> ~/claude_setup_final.txt
echo "=========================" >> ~/claude_setup_final.txt
```

---

## Session Restart Recovery

If you need to restart Claude sessions during this process:

### Check Current Progress:
```bash
# Check which phase you're on by testing current state
claude --version                    # What version is active?
which claude                        # Where is it installed?
ls -la ~/.local/bin/claude 2>/dev/null || echo "No native binary"  # Phase 1 complete?
brew list | grep claude            # Homebrew installed?
echo $PATH | grep homebrew          # PATH configured?
```

### Resume from Phase:
- **Phase 1**: If `claude --version` shows 1.0.72 and native binary exists
- **Phase 2**: If cleanup done but still old version  
- **Phase 3**: If homebrew installed but PATH issues
- **Phase 4**: If everything installed but need verification

---

## Troubleshooting

### Problem: "Command not found: claude"
- Run: `brew reinstall anthropic-ai/claude/claude-code`
- Check: `echo $PATH | grep homebrew`
- Fix: `source ~/.zprofile`

### Problem: Still showing old version
- Check: `which -a claude` (shows all claude installations)
- Remove other installations manually
- Restart terminal completely

### Problem: brew tap fails
- Run: `brew tap --repair`
- Try: `brew tap anthropic-ai/claude https://github.com/anthropics/homebrew-claude.git`

### Rollback Plan
If something goes wrong, restore from backup:
```bash
# Restore original .zprofile
cp ~/.zprofile.backup ~/.zprofile

# Remove homebrew installation
brew uninstall claude-code

# You'll need to reinstall your preferred method
```

---

## Final Notes

- **Preferred Method**: Homebrew for standard macOS package management
- **Configuration File**: .zprofile for your shell customizations  
- **Update Method**: `brew upgrade claude-code` for future updates
- **Backup**: Keep `~/.zprofile.backup` for safety

This guide ensures a clean, standard Claude Code installation using homebrew as the single source of truth, with all your customizations properly configured in .zprofile.
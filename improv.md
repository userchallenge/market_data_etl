Lägg till i: ~/.claude/CLAUDE.md

## Python Environment
- **Always use the current venv Python 3.x that you can derive from virtual environment**: `source venv/bin/activate && python`
- **Never use system Python** (python3 command) - it causes SSL warnings and version conflicts
- Virtual environment location: `./venv/`

## Other packages and tools
- **Always use pytest**
- **For databases, use SQLite and SQLAlchemy**


## Default architecture principles
- **Any data collection and storage should follow ETL-pattern**
- **Always separate data collection and analysis**

## Development process

# Analyse task
- Read existing code in repo
- Read other referrals such as files, repos, websites etc
- Use best practices, widely accepted standards for coding and approach for the particular task
- Ask questions and suggest improvements according to best practices

# Create implementation plan
- Use findings from Analyse step to create plan
- Check CW-Coding-Style below to ensure that you're suggested implementation plan follows guidelines. You must always present contracdictions, suggest way forward and wait for approval if there are contradictions.
- Present goal of implementation
- List the numbered key steps of the plan. Substeps of one level can be used, e.g. 1 -> 1.1, 1.2 for easy reference
- List numbered deliverables
- Keep plan presentation short and only include what's actuall is being done without argumentation for why it is good.
- Think hard to verify the reasonableness of the suggested plan
Highlight risks with the suggested implementation plan and how these risks could be mitigated
- Ask for approval to go ahead

# Approval-step
- Uppdate plan according to suggestions
- Finalize the plan
- Present plan again for approval

# Implement
- Analyse existing pytests
- Create pytest files for new functionality
- Write code according to heading CW-Coding-Style below

# Verify
Run pytests, both existing and new
Give user suggestion on what to test from CLI or simple *.py-files. Provide list of CLI-commands if they exist and/or point to *.py-file with tests
Ask for approval for clean up-phase

# Clean up
Analyse the whole code base, both existing and newly created
identify non-used code, database tables or columns, config files or similar
Update Readme.md to include up to date functionality
Ensure CLI-functionality only contains working commands

## CW-Coding-Style
1. Create small, focused functions
2. Analyse existing functions and reuse and abstract when possible to enable reuse of code 
3. Use constants and configuration files
4. Be sure to use clear and consistent Class, variable and function names that can be understood by anyone. Good examples are DbManager, yearly_int_rate, car_name, load_pricing_data. Bad examples are x, y, Manager, load, ctx
5. Simplify complex conditionals with early returns
6. Use basic error handling without assuming any defaults to make it easy to identify errors
7. Organize imports and remove unused code
8. Add short and to-the-point comments for complex logic
9. Keep CLI-commands in the same file and updated with every new taks or change
10. Create or update doc strings so that they are relevant and short and crisp



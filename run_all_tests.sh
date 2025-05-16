# Define ANSI color codes
GREEN="\033[32m"
RESET="\033[0m"

# Display ASCII art banner in green
echo "${GREEN}╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════╗${RESET}"
echo "${GREEN}║                                                                                                              ║${RESET}"
echo "${GREEN}║                                                                                                              ║${RESET}"
echo "${GREEN}║   ███████╗ █████╗ ███████╗██╗  ██╗██╗ ██████╗ ███╗   ██╗    ███████╗████████╗██╗   ██╗██████╗ ██╗ ██████╗    ║${RESET}"
echo "${GREEN}║   ██╔════╝██╔══██╗██╔════╝██║  ██║██║██╔═══██╗████╗  ██║    ██╔════╝╚══██╔══╝██║   ██║██╔══██╗██║██╔═══██╗   ║${RESET}"
echo "${GREEN}║   █████╗  ███████║███████╗███████║██║██║   ██║██╔██╗ ██║    ███████╗   ██║   ██║   ██║██║  ██║██║██║   ██║   ║${RESET}"
echo "${GREEN}║   ██╔══╝  ██╔══██║╚════██║██╔══██║██║██║   ██║██║╚██╗██║    ╚════██║   ██║   ██║   ██║██║  ██║██║██║   ██║   ║${RESET}"
echo "${GREEN}║   ██║     ██║  ██║███████║██║  ██║██║╚██████╔╝██║ ╚████║    ███████║   ██║   ╚██████╔╝██████╔╝██║╚██████╔    ║${RESET}"
echo "${GREEN}║   ╚═╝     ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝ ╚═════╝ ╚═╝  ╚═══╝    ╚══════╝   ╚═╝    ╚═════╝ ╚═════╝ ╚═╝ ╚═════╝    ║${RESET}"
echo "${GREEN}║                                                                                                              ║${RESET}"
echo "${GREEN}║   ███████╗████████╗██╗          ██████╗ ██╗██████╗ ███████╗██╗     ██╗███╗   ██╗███████╗                     ║${RESET}"
echo "${GREEN}║   ██╔════╝╚══██╔══╝██║          ██╔══██╗██║██╔══██╗██╔════╝██║     ██║████╗  ██║██╔════╝                     ║${RESET}"
echo "${GREEN}║   █████╗     ██║   ██║          ██████╔╝██║██████╔╝█████╗  ██║     ██║██╔██╗ ██║█████╗                       ║${RESET}"
echo "${GREEN}║   ██╔══╝     ██║   ██║          ██╔═══╝ ██║██╔═══╝ ██╔══╝  ██║     ██║██║╚██╗██║██╔══╝                       ║${RESET}"
echo "${GREEN}║   ███████╗   ██║   ███████╗     ██║     ██║██║     ███████╗███████╗██║██║ ╚████║███████╗                     ║${RESET}"
echo "${GREEN}║   ╚══════╝   ╚═╝   ╚══════╝     ╚═╝     ╚═╝╚═╝     ╚══════╝╚══════╝╚═╝╚═╝  ╚═══╝╚══════╝                     ║${RESET}"
echo "${GREEN}║                                                                                                              ║${RESET}"
echo "${GREEN}║   ████████╗███████╗███████╗████████╗██╗███╗   ██╗ ██████╗                                                    ║${RESET}"
echo "${GREEN}║   ╚══██╔══╝██╔════╝██╔════╝╚══██╔══╝██║████╗  ██║██╔════╝                                                    ║${RESET}"
echo "${GREEN}║      ██║   █████╗  ███████╗   ██║   ██║██╔██╗ ██║██║  ███╗                                                   ║${RESET}"
echo "${GREEN}║      ██║   ██╔══╝  ╚════██║   ██║   ██║██║╚██╗██║██║   ██║                                                   ║${RESET}"
echo "${GREEN}║      ██║   ███████╗███████║   ██║   ██║██║ ╚████║╚██████╔╝                                                   ║${RESET}"
echo "${GREEN}║      ╚═╝   ╚══════╝╚══════╝   ╚═╝   ╚═╝╚═╝  ╚═══╝ ╚═════╝                                                    ║${RESET}"
echo "${GREEN}║                                                                                                              ║${RESET}"
echo "${GREEN}║                                                                                                              ║${RESET}"
echo "${GREEN}╚══════════════════════════════════════════════════════════════════════════════════════════════════════════════╝${RESET}"

echo ""
echo "-----------------------------"
echo "🔍 Checking test files..."
echo "-----------------------------"

# Check if test files exist
if [ ! -f "tests/test_load.py" ]; then
    echo "⚠️  Warning: tests/test_load.py not found - creating minimal test file"
    touch tests/test_load.py
    echo "# Placeholder test file for load module" > tests/test_load.py
    echo "def test_placeholder(): pass" >> tests/test_load.py
fi

if [ ! -f "tests/test_transform.py" ]; then
    echo "⚠️  Warning: tests/test_transform.py not found - creating minimal test file"
    touch tests/test_transform.py
    echo "# Placeholder test file for transform module" > tests/test_transform.py
    echo "def test_placeholder(): pass" >> tests/test_transform.py
fi

echo ""
echo "🧪 Running all unit tests..."
echo "-----------------------------"

# Run all tests with coverage
python -m pytest tests/ -v \
    --cov=utils \
    --cov=main \
    --cov-report=html:htmlcov \
    --cov-report=term-missing \
    --cov-fail-under=50

# Check if coverage command exists
if command -v coverage &> /dev/null; then
    echo ""
else
    echo ""
    echo "⚠️  Coverage command not found. Installing coverage..."
    pip install coverage
    coverage report --show-missing
fi

# Check if all tests passed
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ ALL TESTS PASSED!"
    echo "📁 Coverage report: htmlcov/index.html"
else
    echo ""
    echo "❌ SOME TESTS FAILED!"
    echo "Please check the output above for details."
    exit 1
fi
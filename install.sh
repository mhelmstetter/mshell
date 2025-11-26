#!/bin/bash

# mshell installation script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the absolute path of the mshell directory
MSHELL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SOURCE_JAR="$MSHELL_DIR/bin/mshell.jar"
INSTALL_DIR="$HOME/.local/bin"

echo "Installing mshell..."

# Check if source JAR exists
if [ ! -f "$SOURCE_JAR" ]; then
    echo -e "${RED}Error: mshell.jar not found at $SOURCE_JAR${NC}"
    echo "Please run 'mvn clean package -DskipTests' first to build the JAR"
    exit 1
fi

# Create install directory if it doesn't exist
if [ ! -d "$INSTALL_DIR" ]; then
    echo "Creating directory $INSTALL_DIR..."
    mkdir -p "$INSTALL_DIR"
fi

# Create wrapper script
echo "Creating mshell wrapper script..."
WRAPPER_SCRIPT="$INSTALL_DIR/mshell"
cat > "$WRAPPER_SCRIPT" << EOF
#!/bin/bash
java -jar "$SOURCE_JAR" "\$@"
EOF
chmod +x "$WRAPPER_SCRIPT"

# Check if ~/.local/bin is in PATH
if ! echo "$PATH" | grep -q "$INSTALL_DIR"; then
    echo ""
    echo -e "${YELLOW}Note: $INSTALL_DIR is not in your PATH${NC}"
    echo ""
    echo "Add it to your PATH by adding this line to your shell configuration file:"

    # Detect shell configuration file
    if [ -n "$ZSH_VERSION" ] || [ -f "$HOME/.zshrc" ]; then
        echo -e "${GREEN}echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.zshrc${NC}"
    elif [ -n "$BASH_VERSION" ] || [ -f "$HOME/.bashrc" ]; then
        echo -e "${GREEN}echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.bashrc${NC}"
    else
        echo -e "${GREEN}echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.profile${NC}"
    fi

    echo ""
    echo "Then reload your shell or run: source ~/.zshrc (or ~/.bashrc)"
fi

echo ""
echo -e "${GREEN}Installation complete!${NC}"
echo ""
echo "mshell has been installed to $INSTALL_DIR/mshell"
echo "You can now run: mshell"
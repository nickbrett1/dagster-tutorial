FROM mcr.microsoft.com/devcontainers/python
# Node is NOT installed from apt here: devcontainer.json already installs it
# through the devcontainers "node" feature (nvm). A second, apt-provided Node
# puts Debian's own corepack at /usr/share/nodejs/corepack, and the feature's
# `corepack enable` then picks that old corepack up instead of the nvm-bundled
# one and dies (devcontainer-features-install.sh exit 1) - the whole image
# build fails:
#   EACCES: permission denied, symlink '../share/nodejs/corepack/dist/pnpm.js'
#     -> '/usr/bin/pnpm'
# The node feature's PATH entry is what makes node/npm available to the
# post-create scripts, so nothing here needs the apt package.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends git socat curl gnupg



USER vscode

RUN if [ -d "$HOME/.oh-my-zsh" ]; then rm -rf "$HOME/.oh-my-zsh"; fi \
    && sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended \
    && git clone https://github.com/zsh-users/zsh-syntax-highlighting.git $HOME/.oh-my-zsh/custom/plugins/zsh-syntax-highlighting \
    && git clone https://github.com/zsh-users/zsh-autosuggestions $HOME/.oh-my-zsh/custom/plugins/zsh-autosuggestions \
    && git clone --depth=1 https://github.com/romkatv/powerlevel10k.git $HOME/.oh-my-zsh/custom/themes/powerlevel10k \
    && curl https://cursor.com/install -fsS | bash

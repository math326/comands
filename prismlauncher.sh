#!/bin/bash
# Script para instalar dependências do Minecraft Java no Arch Linux

echo "Instalando dependências para Minecraft Java..."

echo Instalando Java (JDK 25)
sudo pacman -S --needed jdk-openjdk

echo instalando Bibliotecas gráficas e de sistema
sudo pacman -S --needed libx11 libxext libxrandr libxss libxcb openal mesa

echo instalando Rede e integração
sudo pacman -S --needed curl gtk3 libnotify libsecret nss

echo instalando Ferramentas para compilar pacotes do AUR
sudo pacman -S --needed base-devel fakeroot debugedit

echo instalando Integração com navegador (login Microsoft no Prism Launcher)
sudo pacman -S --needed xdg-utils

sudo pacman -S yay

echo "Todas as dependências foram instaladas!"
echo "Agora você pode instalar o Prism Launcher com: yay -S prismlauncher"
yay -S prismlauncher


echo os mundos ficam nesse local:

echo ~/.local/share/PrismLauncher/instances/1.21.11/minecraft/saves/


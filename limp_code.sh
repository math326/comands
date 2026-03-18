#!/bin/bash

echo Apaga o cache de armazenamento das áreas de trabalho
rm -rf ~/.config/Code/User/WorkspaceStorage/

echo Apaga o cache de Service Workers (provável culpado)
rm -rf ~/.config/Code/Service\ Worker/

echo Apaga o cache geral de Webviews
rm -rf ~/.config/Code/CachedData/

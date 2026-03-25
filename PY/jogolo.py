import os
import sys
import random
import time
from collections import deque

# -------- utilitários --------
def clear():
    os.system('cls' if os.name == 'nt' else 'clear')

# -------- checagem de caminho (considerando deslize) --------
def path_exists(mapa, start, end):
    """Verifica se existe caminho entre start e end (BFS com deslize)."""
    h, w = len(mapa), len(mapa[0])
    q = deque([start])
    seen = {start}

    while q:
        x, y = q.popleft()
        if (x, y) == end:
            return True

        # tentar deslizar em todas as direções
        for dx, dy in [(-1,0),(1,0),(0,-1),(0,1)]:
            nx, ny = x, y
            while True:
                tx, ty = nx + dx, ny + dy
                if mapa[tx][ty] == '#':  # bateu na parede
                    break
                nx, ny = tx, ty
                if (nx, ny) == end:
                    return True
            if (nx, ny) not in seen:
                seen.add((nx, ny))
                q.append((nx, ny))

    return False

# -------- geração da fase --------
def gerar_fase(nivel, largura=13, altura=11):
    """Gera uma fase garantidamente jogável, sem travar no início ou fim."""
    while True:
        mapa = [['.' for _ in range(largura)] for _ in range(altura)]

        # bordas
        for i in range(largura):
            mapa[0][i] = '#'
            mapa[altura-1][i] = '#'
        for j in range(altura):
            mapa[j][0] = '#'
            mapa[j][largura-1] = '#'

        start = (1, 1)
        end = (altura-2, largura-2)

        # zonas seguras: começo, fim e vizinhos
        safe_zone = {
            start, end,
            (1, 2), (2, 1), (2, 2),
            (altura-3, largura-2), (altura-2, largura-3), (altura-3, largura-3)
        }

        # paredes internas, aumentando gradualmente com o nível
        num_walls = min(3 + nivel*3, (largura-2)*(altura-2)-len(safe_zone))
        for _ in range(num_walls):
            x, y = random.randint(1, altura-2), random.randint(1, largura-2)
            if (x, y) in safe_zone:
                continue
            mapa[x][y] = '#'

        mapa[start[0]][start[1]] = 'M'  # jogador
        mapa[end[0]][end[1]] = '$'      # saída

        if path_exists(mapa, start, end):
            return mapa, start, end

# -------- exibição e movimento --------
def mostrar_mapa(mapa):
    for linha in mapa:
        print(" ".join(linha))

def mover(mapa, pos, direcao):
    x, y = pos
    dx, dy = 0, 0
    if direcao == 'UP': dx = -1
    elif direcao == 'DOWN': dx = 1
    elif direcao == 'LEFT': dy = -1
    elif direcao == 'RIGHT': dy = 1
    else: return pos

    if mapa[x][y] == 'M':
        mapa[x][y] = '.'

    while True:
        nx, ny = x+dx, y+dy
        if mapa[nx][ny] == '#':
            break
        x, y = nx, ny
        if mapa[x][y] == '$':
            break

    if mapa[x][y] != '$':
        mapa[x][y] = 'M'
    return (x,y)

# -------- captura das teclas --------
def get_key():
    """Captura tecla (setas ou q)."""
    if os.name == 'nt':  # Windows
        import msvcrt
        while True:
            ch = msvcrt.getch()
            if ch in (b'\x00', b'\xe0'):
                ch2 = msvcrt.getch()
                if ch2 == b'H': return 'UP'
                if ch2 == b'P': return 'DOWN'
                if ch2 == b'K': return 'LEFT'
                if ch2 == b'M': return 'RIGHT'
            else:
                if ch in (b'q', b'Q'): return 'QUIT'
    else:  # Linux / Mac
        import tty, termios
        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            ch = sys.stdin.read(1)
            if ch == '\x1b':
                seq = sys.stdin.read(2)
                if seq == '[A': return 'UP'
                if seq == '[B': return 'DOWN'
                if seq == '[C': return 'RIGHT'
                if seq == '[D': return 'LEFT'
            elif ch in ('q','Q'):
                return 'QUIT'
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)

# -------- loop principal --------
def jogar():
    print("Tomb of the Mask (Terminal)")
    print("Controle com as setas. Q para sair.")
    time.sleep(1)

    for nivel in range(1, 51):  # 50 fases
        mapa, pos, saida = gerar_fase(nivel)
        while pos != saida:
            clear()
            print(f"=== Fase {nivel} ===")
            mostrar_mapa(mapa)
            key = get_key()
            if key == 'QUIT':
                sys.exit(0)
            if key:
                pos = mover(mapa, pos, key)

        clear()
        print(f"Você passou da fase {nivel}!")
        time.sleep(0.5)

    print("=================")
    print("Beautiful!")
    print("=================")

if __name__ == "__main__":
    jogar()

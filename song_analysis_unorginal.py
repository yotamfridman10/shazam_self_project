from librosa import load, pyin, yin
from numpy import log2, array, std, corrcoef, prod
from collections import defaultdict
from db import insert_many_windows, get_all_windows_by_song

def pitch(song_file):
    waveF, sr = load(song_file)
    f0, voiced_flag, voiced_probs = pyin(waveF, fmin=80, fmax=1000, frame_length=2048, hop_length=256)
    frequency_per_time = f0[voiced_flag]

    # f0 = librosa.yin(waveF, fmin=80, fmax=1000, frame_length=2048, hop_length=256)
    # frequency_per_time = f0[f0 > 0]

    return frequency_per_time


def MIDI(f):
    return 69 + 12 * log2(f / 440.0)


def create_line(frequency_per_time):
    notes = [round(MIDI(f)) for f in frequency_per_time]
    clean_notes = [notes[0]]

    for i in range(1, len(notes)):
        if notes[i] != notes[i - 1]:
            clean_notes.append(notes[i])

    return notes

def relative_defference(line):
    relative = []

    for i in range(1, len(line)):
        diff = line[i] - line[i-1]
        if abs(diff) <= 12:
            relative.append(diff)

    return relative
    

def create_windows_for_song(relative, song_name, window_range=20):
    windows = []
    relative_window = []
    window = ()

    for i in range(0, len(relative) - window_range + 1):
        for j in range(0, window_range):
            relative_window.append(relative[i + j])
        window = (song_name, relative_window)
        windows.append(window)
        relative_window = []

    return windows


def save_windows_of_song_in_db(conn, windows):
    insert_many_windows(conn, windows)


def split_list(list, n=20):
    j = 0
    for i in range(0, len(list), n):
        j = i + n
        yield list[i : i + n]
    if j < len(list):
        yield list[j : len(list)]


def calc_const_editing_distance(lst1, lst2):
    delta = 0
    for i in range(0, len(lst1)):
        delta += (lst1[i] - lst2[i]) ** 2
    return delta    


def edit_distance(seq1, seq2):
    n, m = len(seq1), len(seq2)
    dp = [[0.0] * (m + 1) for _ in range(n + 1)]
    
    for i in range(n + 1): dp[i][0] = i
    for j in range(m + 1): dp[0][j] = j
    
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            diff = abs(seq1[i-1] - seq2[j-1])
            match_cost = diff / 12  # נרמול לפי אוקטבה
            dp[i][j] = min(
                dp[i-1][j] + 1,
                dp[i][j-1] + 1,
                dp[i-1][j-1] + match_cost
            )
    
    return dp[n][m]


def correlation(a, b):
    a = array(a)
    b = array(b)

    if len(a) != len(b):
        return 0

    if std(a) == 0 or std(b) == 0:
        return 0

    return corrcoef(a, b)[0, 1]


def comparing_windows_by_edit_distance(conn, relative, window_range=20):
    if len(relative) == 0: 
        return None
    
    best_match = {}
    ed = 0 
    min_ed = 1000000
    min_ed_song = 1000000
    best_song_match = ""
 
    windows = list(split_list(relative))           
    for song_name, song_windows in get_all_windows_by_song(conn):
        for i in range(0, len(song_windows) - len(windows) +1):
            for j in range(0, len(windows) - 1):
                ed += edit_distance(song_windows[i + j], windows[j])
            x = len(windows) - 1
            ed += edit_distance(windows[x], (song_windows[i + x])[:len(windows[x])])
            if ed < min_ed:
                min_ed = ed
            ed = 0
        best_match[song_name] = min_ed
        if min_ed < min_ed_song:
            min_ed_song = min_ed
            best_song_match = song_name
        min_ed = 1000000

    return best_match, best_song_match


def comparing_windows_by_correlation(conn, relative, window_range=20):
    if len(relative) == 0: 
        return None
    
    best_match = {}
    cor = 0 
    max_cor = 0
    max_cor_song = 0
    best_song_match = ""
 
    windows = list(split_list(relative))           
    for song_name, song_windows in get_all_windows_by_song(conn):
        for i in range(0, len(song_windows) - len(windows) +1):
            for j in range(0, len(windows) - 1):
                if correlation(song_windows[i + j], windows[j]) > 0.6:
                    cor += correlation(song_windows[i + j], windows[j]) ** 3
            x = len(windows) - 1
            if correlation(windows[x], (song_windows[i + x])[:len(windows[x])]) > 0.6:
                cor += correlation(windows[x], (song_windows[i + x])[:len(windows[x])]) ** 3
            if cor > max_cor:
                max_cor = cor
            cor = 0
        best_match[song_name] = max_cor
        if max_cor > max_cor_song:
            max_cor_song = max_cor
            best_song_match = song_name
        max_cor = 0

    return best_match, best_song_match


def comparing_windows_geometric_correlation(conn, relative, window_range=20):
    if len(relative) == 0:
        return None

    best_match = {}
    max_cor_song = 0
    best_song_match = ""
    windows = list(split_list(relative))

    for song_name, song_windows in get_all_windows_by_song(conn):
        max_cor = 0

        for i in range(0, len(song_windows) - len(windows) + 1):
            cors = []
            for j in range(0, len(windows) - 1):
                c = correlation(song_windows[i + j], windows[j])
                if c > 0:  # רק קורלציות חיוביות
                    cors.append(c)
            x = len(windows) - 1
            c = correlation(windows[x], (song_windows[i + x])[:len(windows[x])])
            if c > 0:
                cors.append(c)

            # לפחות חצי מהחלונות צריכים להיות טובים
            if len(cors) >= len(windows) // 2:
                cor = prod(cors) ** (1 / len(cors))
                if cor > max_cor:
                    max_cor = cor

        best_match[song_name] = max_cor
        if max_cor > max_cor_song:
            max_cor_song = max_cor
            best_song_match = song_name

    return best_match, best_song_match


def comparing_windows_min_correlation(conn, relative, window_range=20):
    if len(relative) == 0:
        return None

    best_match = {}
    max_cor_song = 0
    best_song_match = ""
    windows = list(split_list(relative))

    for song_name, song_windows in get_all_windows_by_song(conn):
        max_cor = 0

        for i in range(0, len(song_windows) - len(windows) + 1):
            cors = []
            for j in range(0, len(windows) - 1):
                cors.append(correlation(song_windows[i + j], windows[j]))
            x = len(windows) - 1
            cors.append(correlation(windows[x], (song_windows[i + x])[:len(windows[x])]))

            # רק קורלציות חיוביות, לפחות חצי
            positive = [c for c in cors if c > 0]
            if len(positive) >= len(windows) // 2:
                cor = min(positive)  # המינימום רק מהחיוביים
                if cor > max_cor:
                    max_cor = cor

        best_match[song_name] = max_cor
        if max_cor > max_cor_song:
            max_cor_song = max_cor
            best_song_match = song_name

    return best_match, best_song_match


def pipeline(song_file):
    frequency_per_time = pitch(song_file)
    notes = create_line(frequency_per_time)
    relative = relative_defference(notes)
    return relative
    

def analyze_new_song(conn, song_file, song_name):
    print(f"[index] {song_name}")
    relative = pipeline(song_file)
    windows = create_windows_for_song(relative, song_name)
    save_windows_of_song_in_db(conn, windows)
    print(f"  stored OK")


def analyze_query_song(song_file, conn):
    print(f"[query] {song_file}")
    relative = pipeline(song_file)
    matches, best_song_match = comparing_windows_by_edit_distance(conn, relative)
    print(f"  matches: {matches}")
    return best_song_match
    
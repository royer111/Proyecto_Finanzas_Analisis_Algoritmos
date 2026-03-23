"""
sorting_algorithms.py
---------------------
Implementación explícita de 12 algoritmos de ordenamiento.
Cada función recibe una lista de tuplas y una clave de comparación,
y retorna la lista ordenada de forma ASCENDENTE.

Complejidades de referencia:
  TimSort              O(n log n)
  Comb Sort            O(n log n)
  Selection Sort       O(n²)
  Tree Sort            O(n log n)
  Pigeonhole Sort      O(n + range)
  Bucket Sort          O(n + k)
  QuickSort            O(n log n)
  HeapSort             O(n log n)
  Bitonic Sort         O(n log² n)
  Gnome Sort           O(n²)
  Binary Insertion Sort O(n²)
  Radix Sort           O(nk)
"""

import math


# ---------------------------------------------------------------------------
# 1. TimSort  —  O(n log n)
#    Python's built-in sort under the hood; aquí lo exponemos explícitamente
#    usando la misma lógica de merge de runs naturales.
# ---------------------------------------------------------------------------
def tim_sort(arr, key=lambda x: x):
    RUN = 32

    def insertion_sort_run(a, left, right):
        for i in range(left + 1, right + 1):
            temp = a[i]
            j = i - 1
            while j >= left and key(a[j]) > key(temp):
                a[j + 1] = a[j]
                j -= 1
            a[j + 1] = temp

    def merge(a, left, mid, right):
        left_part = a[left: mid + 1]
        right_part = a[mid + 1: right + 1]
        i = j = 0
        k = left
        while i < len(left_part) and j < len(right_part):
            if key(left_part[i]) <= key(right_part[j]):
                a[k] = left_part[i]; i += 1
            else:
                a[k] = right_part[j]; j += 1
            k += 1
        while i < len(left_part):
            a[k] = left_part[i]; i += 1; k += 1
        while j < len(right_part):
            a[k] = right_part[j]; j += 1; k += 1

    data = list(arr)
    n = len(data)
    for start in range(0, n, RUN):
        end = min(start + RUN - 1, n - 1)
        insertion_sort_run(data, start, end)
    size = RUN
    while size < n:
        for left in range(0, n, 2 * size):
            mid = min(left + size - 1, n - 1)
            right = min(left + 2 * size - 1, n - 1)
            if mid < right:
                merge(data, left, mid, right)
        size *= 2
    return data


# ---------------------------------------------------------------------------
# 2. Comb Sort  —  O(n log n) promedio
# ---------------------------------------------------------------------------
def comb_sort(arr, key=lambda x: x):
    data = list(arr)
    n = len(data)
    gap = n
    shrink = 1.3
    sorted_ = False
    while not sorted_:
        gap = int(gap / shrink)
        if gap <= 1:
            gap = 1
            sorted_ = True
        i = 0
        while i + gap < n:
            if key(data[i]) > key(data[i + gap]):
                data[i], data[i + gap] = data[i + gap], data[i]
                sorted_ = False
            i += 1
    return data


# ---------------------------------------------------------------------------
# 3. Selection Sort  —  O(n²)
# ---------------------------------------------------------------------------
def selection_sort(arr, key=lambda x: x):
    data = list(arr)
    n = len(data)
    for i in range(n):
        min_idx = i
        for j in range(i + 1, n):
            if key(data[j]) < key(data[min_idx]):
                min_idx = j
        data[i], data[min_idx] = data[min_idx], data[i]
    return data


# ---------------------------------------------------------------------------
# 4. Tree Sort  —  O(n log n)
#    BST manual (sin librerías de árbol)
# ---------------------------------------------------------------------------
def tree_sort(arr, key=lambda x: x):
    """
    Tree Sort con BST balanceado (AVL).
    Insercion iterativa O(log n) garantizada, evita degeneracion
    con datos parcialmente ordenados (caso comun en series financieras).
    """
    class Node:
        __slots__ = ("val", "left", "right", "height")
        def __init__(self, v):
            self.val = v
            self.left = self.right = None
            self.height = 1

    def _h(n):
        return n.height if n else 0

    def _update_h(n):
        n.height = 1 + max(_h(n.left), _h(n.right))

    def _rotate_right(y):
        x = y.left
        y.left = x.right
        x.right = y
        _update_h(y); _update_h(x)
        return x

    def _rotate_left(x):
        y = x.right
        x.right = y.left
        y.left = x
        _update_h(x); _update_h(y)
        return y

    def _balance(n):
        bf = _h(n.left) - _h(n.right)
        if bf > 1:
            if _h(n.left.left) < _h(n.left.right):
                n.left = _rotate_left(n.left)
            return _rotate_right(n)
        if bf < -1:
            if _h(n.right.right) < _h(n.right.left):
                n.right = _rotate_right(n.right)
            return _rotate_left(n)
        _update_h(n)
        return n

    def insert(root, val):
        if root is None:
            return Node(val)
        if key(val) < key(root.val):
            root.left = insert(root.left, val)
        else:
            root.right = insert(root.right, val)
        return _balance(root)

    def inorder(root):
        result, stack = [], []
        current = root
        while stack or current:
            while current:
                stack.append(current)
                current = current.left
            current = stack.pop()
            result.append(current.val)
            current = current.right
        return result

    import sys
    sys.setrecursionlimit(max(50000, len(arr) * 3))
    root = None
    for item in arr:
        root = insert(root, item)
    return inorder(root)


# ---------------------------------------------------------------------------
# 5. Pigeonhole Sort  —  O(n + range)
#    Aplica sobre valores numéricos enteros; aquí se usa sobre precios/fechas
#    mapeados a enteros (epoch days o precio * 100).
# ---------------------------------------------------------------------------
def pigeonhole_sort(arr, key=lambda x: x):
    if not arr:
        return []
    data = list(arr)
    keys = [key(x) for x in data]
    min_k, max_k = min(keys), max(keys)
    size = max_k - min_k + 1
    holes = [[] for _ in range(size)]
    for item in data:
        holes[key(item) - min_k].append(item)
    result = []
    for hole in holes:
        result.extend(hole)
    return result


# ---------------------------------------------------------------------------
# 6. Bucket Sort  —  O(n + k)
# ---------------------------------------------------------------------------
def bucket_sort(arr, key=lambda x: x):
    if not arr:
        return []
    data = list(arr)

    # Normaliza la clave a un número flotante para poder hacer aritmética.
    # Si la clave ya devuelve una tupla (clave compuesta), la convertimos a
    # un escalar usando la representación ordinal del primer elemento y el
    # segundo elemento como fracción decimal.
    def _numeric_key(item):
        k = key(item)
        if isinstance(k, tuple):
            # Escala: primer componente * 1e6 + segundo componente
            return k[0] * 1_000_000 + (k[1] if len(k) > 1 else 0)
        return float(k)

    keys = [_numeric_key(x) for x in data]
    min_k, max_k = min(keys), max(keys)
    if min_k == max_k:
        return data
    n = len(data)
    bucket_count = max(1, int(math.sqrt(n)))
    buckets = [[] for _ in range(bucket_count)]
    range_k = max_k - min_k
    for item in data:
        nk = _numeric_key(item)
        idx = int((nk - min_k) / range_k * (bucket_count - 1))
        idx = min(idx, bucket_count - 1)
        buckets[idx].append(item)
    result = []
    for b in buckets:
        b.sort(key=key)
        result.extend(b)
    return result


# ---------------------------------------------------------------------------
# 7. QuickSort  —  O(n log n) promedio, O(n²) peor caso
#    Iterativo para evitar stack overflow en grandes datasets
# ---------------------------------------------------------------------------
def quick_sort(arr, key=lambda x: x):
    """
    QuickSort iterativo con pivot median-of-three.
    Evita la degeneracion O(n^2) con datos casi ordenados,
    que es el caso tipico de series financieras ordenadas por fecha.
    """
    data = list(arr)

    def median_of_three(a, low, high):
        mid = (low + high) // 2
        # Ordena los tres candidatos in-place y devuelve el indice del pivot
        if key(a[low]) > key(a[mid]):
            a[low], a[mid] = a[mid], a[low]
        if key(a[low]) > key(a[high]):
            a[low], a[high] = a[high], a[low]
        if key(a[mid]) > key(a[high]):
            a[mid], a[high] = a[high], a[mid]
        # Coloca el pivot (mediana) en high-1
        a[mid], a[high - 1] = a[high - 1], a[mid]
        return high - 1

    def partition(a, low, high):
        if high - low < 3:
            # Para subarreglos muy pequenos usa insertion sort
            for i in range(low + 1, high + 1):
                tmp = a[i]; j = i - 1
                while j >= low and key(a[j]) > key(tmp):
                    a[j + 1] = a[j]; j -= 1
                a[j + 1] = tmp
            return (low + high) // 2
        pivot_idx = median_of_three(a, low, high)
        pivot = key(a[pivot_idx])
        i = low - 1
        for j in range(low, high):
            if j == pivot_idx:
                continue
            if key(a[j]) <= pivot:
                i += 1
                if i == pivot_idx:
                    pivot_idx = j
                a[i], a[j] = a[j], a[i]
        i += 1
        a[i], a[pivot_idx] = a[pivot_idx], a[i]
        return i

    stack = [(0, len(data) - 1)]
    while stack:
        low, high = stack.pop()
        if low < high:
            pi = partition(data, low, high)
            stack.append((low, pi - 1))
            stack.append((pi + 1, high))
    return data


# ---------------------------------------------------------------------------
# 8. HeapSort  —  O(n log n)
# ---------------------------------------------------------------------------
def heap_sort(arr, key=lambda x: x):
    data = list(arr)
    n = len(data)

    def heapify(a, n, i):
        largest = i
        l, r = 2 * i + 1, 2 * i + 2
        if l < n and key(a[l]) > key(a[largest]):
            largest = l
        if r < n and key(a[r]) > key(a[largest]):
            largest = r
        if largest != i:
            a[i], a[largest] = a[largest], a[i]
            heapify(a, n, largest)

    for i in range(n // 2 - 1, -1, -1):
        heapify(data, n, i)
    for i in range(n - 1, 0, -1):
        data[0], data[i] = data[i], data[0]
        heapify(data, i, 0)
    return data


# ---------------------------------------------------------------------------
# 9. Bitonic Sort  —  O(n log² n)  — requiere n = potencia de 2; se rellena
# ---------------------------------------------------------------------------
def bitonic_sort(arr, key=lambda x: x):
    data = list(arr)
    n = len(data)
    # Pad hasta la siguiente potencia de 2
    pad_n = 1
    while pad_n < n:
        pad_n <<= 1
    # Valor sentinela grande para el relleno — compatible con claves tuple o numéricas
    max_key = key(max(data, key=key))
    if isinstance(max_key, tuple):
        sentinel_key = tuple(v + 10**9 for v in max_key)
    else:
        sentinel_key = max_key + 1

    class _Sentinel:
        def __init__(self): self._k = sentinel_key
    sentinels = [_Sentinel() for _ in range(pad_n - n)]
    padded = data + sentinels
    key_padded = lambda x: x._k if isinstance(x, _Sentinel) else key(x)

    def compare_swap(a, i, j, asc):
        if (key_padded(a[i]) > key_padded(a[j])) == asc:
            a[i], a[j] = a[j], a[i]

    def bitonic_merge(a, lo, cnt, asc):
        if cnt > 1:
            k = cnt // 2
            for i in range(lo, lo + k):
                compare_swap(a, i, i + k, asc)
            bitonic_merge(a, lo, k, asc)
            bitonic_merge(a, lo + k, k, asc)

    def bitonic_seq(a, lo, cnt, asc):
        if cnt > 1:
            k = cnt // 2
            bitonic_seq(a, lo, k, True)
            bitonic_seq(a, lo + k, k, False)
            bitonic_merge(a, lo, cnt, asc)

    import sys
    sys.setrecursionlimit(max(10000, pad_n * 4))
    bitonic_seq(padded, 0, pad_n, True)
    return [x for x in padded if not isinstance(x, _Sentinel)]


# ---------------------------------------------------------------------------
# 10. Gnome Sort  —  O(n²)
# ---------------------------------------------------------------------------
def gnome_sort(arr, key=lambda x: x):
    data = list(arr)
    n = len(data)
    pos = 0
    while pos < n:
        if pos == 0 or key(data[pos]) >= key(data[pos - 1]):
            pos += 1
        else:
            data[pos], data[pos - 1] = data[pos - 1], data[pos]
            pos -= 1
    return data


# ---------------------------------------------------------------------------
# 11. Binary Insertion Sort  —  O(n²)  (O(n log n) comparaciones, O(n²) shifts)
# ---------------------------------------------------------------------------
def binary_insertion_sort(arr, key=lambda x: x):
    data = list(arr)
    for i in range(1, len(data)):
        item = data[i]
        lo, hi = 0, i
        while lo < hi:
            mid = (lo + hi) // 2
            if key(data[mid]) <= key(item):
                lo = mid + 1
            else:
                hi = mid
        data[lo + 1: i + 1] = data[lo:i]
        data[lo] = item
    return data


# ---------------------------------------------------------------------------
# 12. Radix Sort  —  O(nk)
#    Trabaja sobre claves enteras no negativas (se usa epoch-day o precio*100)
# ---------------------------------------------------------------------------
def radix_sort(arr, key=lambda x: x):
    if not arr:
        return []
    data = list(arr)
    keys = [key(x) for x in data]
    max_k = max(keys)

    def counting_sort_by_digit(a, ks, exp):
        n = len(a)
        output = [None] * n
        count = [0] * 10
        for k in ks:
            count[(k // exp) % 10] += 1
        for i in range(1, 10):
            count[i] += count[i - 1]
        for i in range(n - 1, -1, -1):
            digit = (ks[i] // exp) % 10
            count[digit] -= 1
            output[count[digit]] = a[i]
        new_keys = [key(x) for x in output]
        return output, new_keys

    exp = 1
    while max_k // exp > 0:
        data, keys = counting_sort_by_digit(data, keys, exp)
        exp *= 10
    return data
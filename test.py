import re

def clean_data(inp: str) -> str:
    '''
        Избавляемся от комментариев. в TS комментарии представлены
        двумя возможными конструкциями:
        1) // Однострочный комментарий
        2) /* Вариант
            * Многострочного
            * Комментария
            */
    '''
    inp = re.sub(r"(\/\/*[\s\S\n]*?\*\/)|(\/\/.*?\n)", '', inp)

    # Педварительно обработаем некоторые случаи внутри классов
    inp = re.sub(r'\b(public|private|protected)\b', 'const', inp)
    inp = re.sub(r'\bthis\.', '', inp)

    '''
        В особенности языка входит возможность разный сигнатур функций
        Например:
        1) function func(x, a) {
            x = x + a
            return x * 2
        }
        2) const func = (x, a) => {
            return (x + a) * 2
        }
        3) let func = (x, a) => (x + a) * 2

        Все обозначенные функции выполняют одно и тоже,
        но в первом случае используется стандартное объявление,
        во втором - анонимная стрелочная функция,
        в третьем - анонимное функциональное выражение.

        Мы попробуем привести каждое такое объявление к единому стандарту. 
    '''

    # Обработка стрелочных функций. Очень страшное и длинное выражение, но вполне разбираемое
    '''(const|let) # СТрелочная функция объявляется константой или переменной
    \s+([a-zA-Z_$]\w*)\s* #Имя - сигнатура функции. Является первой группой
    (?::\s*\w+\s*)? # Опционально может появится тип возвращаемой функции
    =\s # присвоить...
    *(?:\(?)
    ([^\)\n]*) # здесь располагаются параметры функции. Они могут быть как в скобках так и без
    (?:\)?)\s* 
    =>\s* # Стрелочная функция
    (?:<.*?>)?\s* # Отсекаем generic-функции TS (const func: number = () => <T extends string>{})
    (\{?[^}]*\}?|.+?) # Непосредственно тело функции
     (?=;?$)'''
    arrow_functions = r"(const|let)\s+([a-zA-Z_$]\w*)\s*(?::\s*\w+\s*)?=\s*(?:\(?)([^\)\n]*)(?:\)?)\s*=>\s*(?:<.*?>)?\s*(\{?[^}]*\}?|.+?)"
    inp = re.sub(arrow_functions, r'function \2(\3) \4', inp)
    
    function_decls = re.finditer(
        r'\bfunction\s+([a-zA-Z_$][\w$]*)\s*\(([^)]*)\)',
        inp
    )
    counter = 0
    var_map = {}
    for match in function_decls:
        func_name, params = match.groups()
        if func_name not in var_map:
            var_map[func_name] = f'func{counter}'
            counter += 1
        # Обрабатываем параметры
        for param in re.split(r'\s*,\s*', params):
            if param and param not in var_map:
                var_map[param] = f'var{counter}'
                counter += 1
    
    # Теперь мы хотим стандартизировать объявление переменных.
    var_counter = [0]  # Используем список для mutable состояния в UDF

    declarations = re.finditer(
        r'\b(var|let|const|function|class)\s+([a-zA-Z_$][\w$]*)\b',
        inp
    )
    for match in declarations:
        var_type, var_name = match.groups()
        if var_name not in var_map:
            var_counter[0] += 1
            var_map[var_name] = f'{var_type}{var_counter[0]}'

    def replace_var(match: str):
        word = match.group(0)
        return var_map.get(word, word)
    
    # Обработка объявлений переменных и функций
    inp = re.sub(
        r"\b[a-zA-Z_$][\w$]*\b",
        replace_var,
        inp
    )

    # мелкие изменения, по типу стандартизации кавычек, удаление импортов, экспортов и т.п.
    inp = re.sub(r"['\"']", "'", inp)
    inp = re.sub(r"^\s*(import|export).*?;\s*$", '', inp)
    # Обработаем TS-специфику - аннотации типов
    inp = re.sub(r":\s*\w+", '', inp)

    

    '''
        Удаляем все лишние пробелы из кода, а также стандартизируем
        переносы строк в коде
    '''
    inp = re.sub(r"(\s)+", '  ', inp)
    inp = re.sub(r"\s*[\r\n]+\s*", '\n', inp)

    # обработка функциональных выражений. Это по сути функции,
    # которые содержат только один return в теле
    
    return "".join(inp.split())
    
test_data = """
class Vector2D {
  constructor(public x: number, public y: number) {}

  add(vec: Vector2D): Vector2D {
    return new Vector2D(this.x + vec.x, this.y + vec.y);
  }

  magnitude(): number {
    return Math.sqrt(this.x ** 2 + this.y ** 2);
  }
}

const v1 = new Vector2D(3, 4);
console.log(v1.magnitude());
"""

second_test = """
class Vector2DModified {
  constructor(public a: number, public b: number) {}

  sum(vec: Vector2DModified): Vector2DModified {
    return new Vector2DModified(this.a + vec.a, this.b + vec.b);
  }

  length(): number {
    return Math.hypot(this.a, this.b);
  }
}

const vec = new Vector2DModified(3, 4);
console.log(vec.length());
"""

print(clean_data(test_data))
print('Вторая программа')
print(clean_data(second_test))
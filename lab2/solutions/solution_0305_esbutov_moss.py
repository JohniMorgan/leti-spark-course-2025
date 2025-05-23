from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, sha, lit, udf
from pyspark.sql.types import StringType, ArrayType

from common import SparkContextCommon, run_solution

import re

def clean_data(input_code: str) -> str:
    '''
        Избавляемся от комментариев. в TS комментарии представлены
        двумя возможными конструкциями:
        1) // Однострочный комментарий
        2) /* Вариант
            * Многострочного
            * Комментария
            */
    '''
    input_code = re.sub(r"(\/\/*[\s\S\n]*?\*\/)|(\/\/.*?\n)", '', input_code)

    # Педварительно обработаем некоторые случаи внутри классов
    input_code = re.sub(r'\b(public|private|protected)\b', 'const', input_code)
    input_code = re.sub(r'\bthis\.', '', input_code)

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
    '''(const|let) Cтрелочная функция объявляется константой или переменной
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
    input_code = re.sub(arrow_functions, r'function \2(\3) \4', input_code)
    
    function_decls = re.finditer(
        r'\bfunction\s+([a-zA-Z_$][\w$]*)\s*\(([^)]*)\)',
        input_code
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
        input_code
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
    input_code = re.sub(
        r"\b[a-zA-Z_$][\w$]*\b",
        replace_var,
        input_code
    )

    # мелкие изменения, по типу стандартизации кавычек, удаление импортов, экспортов и т.п.
    input_code = re.sub(r"['\"']", "'", input_code)
    input_code = re.sub(r"^\s*(import|export).*?;\s*$", '', input_code)
    # Обработаем TS-специфику - аннотации типов
    input_code = re.sub(r":\s*\w+", '', input_code)

    

    '''
        Удаляем все лишние пробелы из кода, а также стандартизируем
        переносы строк в коде
    '''
    input_code = re.sub(r"(\s)+", '  ', input_code)
    input_code = re.sub(r"\s*[\r\n]+\s*", '\n', input_code)

    # обработка функциональных выражений. Это по сути функции,
    # которые содержат только один return в теле
    
    return "".join(input_code.split())

def create_n_gramms(input: str) -> list:
    substr_size = 5

    result = []
    for i in range(len(input) - substr_size + 1):
        result.append(input[i : i + substr_size])
    return result

clean_data_udf = udf(lambda input_code: clean_data(input_code), StringType())
n_gramms_udf = udf(lambda inp: create_n_gramms(inp), ArrayType(StringType()))
#
# match
# lhs_author
# rhs_author
#
def solve(common: SparkContextCommon) -> DataFrame:
    common.read_data().withColumn(
        'content',
        n_gramms_udf(clean_data_udf(col('content')))).createOrReplaceTempView('data')
    
    # Разобьём полученный массив на столбец
    common.view('exploded_contents', """
                select
                    author as file_src,
                    explode(content) as exploded_content
                from data
                """)
    # Теперь подсчитаем хэши. Я почитал что их есть просто туча
    # На крипте я реализовывал SHA-1, поэтому тут хочу использовать MD-5
    common.view('hashed_n_gramms', """
                select
                    file_src,
                    md5(exploded_content) as hash
                from exploded_contents
                """)
    # Хэши подсчитаны. Вроде как самый лучший вариант - это через Winnowing
    # Но я не совсем понял как это реализуемо в адекватных рамках
    # Поэтому буду использовать метрику match(T1, T2) = hash_matched_count(T1, T2) / hashes_count(T1)
    # https://docs.google.com/presentation/d/18XDiSa0Q5pvF30Xx1cuHxkZ2KMJKEFxalVwCNups6qQ/edit?slide=id.g30d47188da8_0_187#slide=id.g30d47188da8_0_187

    common.view('file_hash_counts', """
                select
                    file_src,
                    count(distinct hash) as hash_count
                from hashed_n_gramms
                group by file_src
                """)
    common.view('result', """
                select
                    target.file_src as target,
                    source.file_src as source,
                    round(count(distinct target.hash) / min(fhs.hash_count), 2) as plagiarism_coefficient
                from hashed_n_gramms target
                    join hashed_n_gramms source on target.hash = source.hash and not target.file_src = source.file_src
                    join file_hash_counts as fhs on target.file_src = fhs.file_src
                group by target.file_src, source.file_src
                order by plagiarism_coefficient desc
                """)
    # df = inp.withColumn('content', clean_data_udf(col('content')))
    # df.show()
    # res_sql = """
    #     select 
    #         cast(match as Double) as match, 
    #         cast(lhs_author as String) as lhs_author, 
    #         cast(rhs_author as String) as rhs_author
    #     from res
    # """
    return common.spark.sql('select * from result')

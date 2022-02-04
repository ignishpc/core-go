import io
import os
import re
import subprocess
import urllib.request
from concurrent.futures import ThreadPoolExecutor

MPICH_VERSION = None


class CMacro:

    def __init__(self, name, real_tp):
        self.name = name
        self.real_tp = real_tp

    def __repr__(self):
        return self.name + ":" + self.real_tp


class CFunction:

    def __init__(self, rtype, name, args):
        self.rtype = rtype
        self.name = name
        self.args = args


class CArg:
    def __init__(self, tp, name):
        self.tp = tp.strip()
        self.name = name.strip()


class MpiParser:

    def __init__(self, cache=True):
        global MPICH_VERSION
        ############### MACROS ##############
        self._macro_list = list()
        raw_macros = subprocess.check_output(['gcc', '-dM', '-E', '-include', 'mpi.h', '-'], input=None).decode().split("\n")
        raw_macros = list(filter(lambda m: "MPI" in m, raw_macros))
        macro_filter = re.compile("#define ([^()]+) (?:\(?\(?([^()]+)(.+)|\(?([0-9a-fx-]+)\)?)")

        for line in raw_macros:
            result = macro_filter.match(line)

            if not result:
                continue
            name = result.group(1)
            if name in "MPI_FILE_NULL":  # Avoid warning
                self._macro_list.append(CMacro(name, "MPI_File"))
                continue
            if result.group(2) is not None:
                value = result.group(2)
                value2 = result.group(3)
                if value2[0].isalnum():
                    value += value2
                    value2 = " "
                value2 = value2.replace(")", "")
                if value[0].isnumeric() or value[0] == '-':
                    self._macro_list.append(CMacro(name, "int"))
                elif value[0] == '"':
                    if name == "MPICH_VERSION":
                        MPICH_VERSION = value.replace('"', "")
                    self._macro_list.append(CMacro(name, "char *"))
                elif "+" in value:
                    self._macro_list.append(CMacro(name, "int"))
                elif value[-1] == '*':
                    self._macro_list.append(CMacro(name, value))
                elif value2[0].isnumeric() or value2[0] == '-':
                    self._macro_list.append(CMacro(name, value))
            elif result.group(4) is not None:
                self._macro_list.append(CMacro(name, "int"))

        self._macro_list.sort(key=lambda m: m.real_tp)

        ############### FUNTIONS ##############
        file_cache = "functions_cache" + MPICH_VERSION + ".txt"
        if cache and os.path.exists(file_cache):
            with open(file_cache) as cache:
                raw_functions = list(map(lambda l: l.rstrip("\n"), cache.readlines()))
        else:
            raw_functions = list()

            prefix = "https://www.mpich.org/static/docs/v" + MPICH_VERSION + "/www3/"

            with urllib.request.urlopen(prefix + "/index.htm") as fp:
                root = fp.read().decode("utf8")
                urls = re.findall('HREF="(.+)"', root, re.IGNORECASE)

            with ThreadPoolExecutor(20) as workers:
                def work(url):
                    with urllib.request.urlopen(prefix + "/" + url) as fp:
                        page = fp.read().decode("utf8")
                    try:
                        start = page.index("<PRE>") + len("<PRE>")
                        end = page.index("</PRE>", start)
                    except:
                        return None
                    return page[start:end]

                raw_function_list = list(workers.map(work, urls))

            for raw_f in raw_function_list:
                if raw_f is None:
                    continue
                lines = raw_f.split("\n")
                header = ""

                for i in range(len(lines)):
                    line = lines[i].strip()
                    if not line or line.startswith("#"):
                        continue
                    if header and header[-1].isalnum() and line[0].isalnum():
                        header += " "
                    if header and line.endswith(")"):
                        header += line
                        break
                    header += line

                header.replace(", ", ",")
                if "..." not in header:
                    raw_functions.append(header)

            if cache:
                with open(file_cache, "w") as cache:
                    cache.write("\n".join(raw_functions))

        words_removed = ["ROMIO_CONST"]
        self._function_list = list()
        for f in raw_functions:
            for word in words_removed:
                f = f.replace(word, "")
            rsep = f.index(" ")
            nsep = f.index("(", rsep)

            rtype = f[0:rsep]
            name = f[rsep + 1: nsep]
            raw_args = f[nsep + 1: -1]
            args = list()
            self._function_list.append(CFunction(rtype, name, args))
            if raw_args == "void":
                continue
            for arg in raw_args.split(","):
                arg = arg.replace("const ", "")
                if "*" in arg:
                    asep = arg.rindex("*") + 1
                else:
                    asep = arg.rindex(" ")
                arg_name = arg[asep:]
                arg_tp = arg[0:asep]
                if "[" in arg_name:
                    a2sep = arg_name.index("[")
                    arg_tp += arg_name[a2sep:]
                    arg_name = arg_name[0:a2sep]

                args.append(CArg(arg_tp, arg_name))

    def getMacros(self):
        return self._macro_list

    def getFunctions(self):
        return self._function_list

    def getVarTypeAsGo(self, c_type):
        nc = c_type.replace("const ", "")
        nc = nc.replace("[]", "*")
        nc = "C." + nc
        sep = len(nc)
        if "[" in nc:
            sep = min(sep, nc.index("["))
        if "*" in nc:
            sep = min(sep, nc.index("*"))
        if sep < len(nc):
            nc = nc[sep::] + nc[0:sep].strip()
        return nc


if __name__ == '__main__':
    mpi = MpiParser()
    unsafe = False

    c_source = io.StringIO()
    go_source = io.StringIO()

    alias_names = set()
    go_alias = io.StringIO()

    def clearType(tp):
        tp = tp.replace("*", "")
        if "[" in tp:
            tp = tp[:tp.index("[")] + tp[tp.index("]") + 1:]
        return tp

    def goAlias(tp):
        if tp.startswith("C."):
            alias = tp.replace('.', '_')
            real_alias = clearType(alias)
            if real_alias not in alias_names:
                alias_names.add(real_alias)
                go_alias.write("type " + real_alias + " = " + clearType(tp) + "\n")
            return alias
        return tp

    c_source.write("#include <mpi.h>\n")
    c_source.write("\n")
    for macro in mpi.getMacros():
        c_source.write(macro.real_tp + " IGNIS_" + macro.name + " = " + macro.name + ";\n")
        go_type = mpi.getVarTypeAsGo(macro.real_tp)
        if go_type == "*C.void":
            go_type = "unsafe.Pointer"
            unsafe = True

        go_source.write("var " + macro.name + " " + goAlias(go_type) + " = ")
        go_source.write("C.IGNIS_" + macro.name + "\n")

    go_source.write("""
type MpiError struct{
	Code int
}

func (m *MpiError) Error() string {
	return "Mpi error code " + strconv.Itoa(m.Code)
}

func mpi_check(code C.int) *MpiError {
	if code == MPI_SUCCESS {
		return nil
	}
	return &MpiError{int(0)}
}

	""")

    for f in mpi.getFunctions():
        go_source.write("func " + f.name + "(")
        i = 0
        for arg in f.args:
            go_source.write(arg.name + " " + goAlias(mpi.getVarTypeAsGo(arg.tp)))
            i += 1
            if i < len(f.args):
                go_source.write(", ")
        go_source.write(") ")
        if f.rtype == "int":
            go_source.write("*MpiError")
        else:
            go_source.write(goAlias(mpi.getVarTypeAsGo(f.rtype)))
        go_source.write(" {\n   return ")
        if f.rtype == "int":
            go_source.write("mpi_check(")
        go_source.write("C." + f.name + "(")
        i = 0
        for arg in f.args:
            if "void *" == arg.tp:
                go_source.write("unsafe.Pointer(" + arg.name + ")")
                unsafe = True
            else:
                go_source.write(arg.name)
            i += 1
            if i < len(f.args):
                go_source.write(", ")
        if f.rtype == "int":
            go_source.write(")")
        go_source.write(")\n")
        go_source.write("}\n\n")

    with open("mpi.go", "w") as result:
        result.write("// Package mpi generated by " + os.path.basename(__file__))
        result.write(" from mpich v" + MPICH_VERSION + " DO NOT EDIT.\n")
        result.write("package impi\n")
        result.write("/*\n#cgo LDFLAGS: -lmpi\n")
        result.write(c_source.getvalue())
        result.write('*/\nimport "C"\n')
        if unsafe:
            result.write('import "unsafe"\n')
        result.write('import "strconv"\n\n')
        result.write(go_alias.getvalue())
        result.write('\n\n')
        result.write(go_source.getvalue())

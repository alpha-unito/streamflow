#!/bin/bash

# Version of the EESSI software layer to generate the recipe against
TARGET=${1:-"2026.06"}

if [[ -z "${EBROOTEASYBUILD:-}" ]]; then
    echo "error: \$EBROOTEASYBUILD is not set (load the EasyBuild/EESSI-extend module)" >&2
    exit 1
fi

EASYCONFIG_DIR="${EBROOTEASYBUILD}/easybuild/easyconfigs"
SCRIPT_DIRECTORY="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

PYPROJECT="${SCRIPT_DIRECTORY}/../pyproject.toml"
TOOLCHAIN_NAME="gfbf"

# Print the easyconfig file path for <soft> built on toolchain <tc>, i.e. a file
# of the form <soft>-<ver>-<tc>-<tcver>.eb in the EasyBuild easyconfigs tree. The
# tree is organised as <root>/<X>/<Soft>/; candidates are globbed and the one
# with the highest <soft> version wins (the EESSI target builds the newest on the
# newest generation). Only candidates whose version parses as dotted-numeric are
# considered, so files with non-numeric toolchain versions (e.g. ...-gfbf-2023a)
# are skipped without crashing the comparison. Prints the path or nothing.
ec_path() {
    local soft="$1" tc="$2"
    local candidate best bestver ver a b ia ib newer
    best=""
    bestver=""
    for candidate in "${EASYCONFIG_DIR}"/[A-Za-z]/"${soft}"/"${soft}"-*-"${tc}"-*.eb; do
        [[ -e "${candidate}" ]] || continue
        # <soft>-<ver>-<tc>-<tcver>.eb
        ver="$(basename "${candidate}" | sed -E "s/^${soft}-([0-9][0-9.]*)-${tc}-[0-9][0-9.]*\\.eb$/\\1/")"
        # sed leaves the line unchanged when the pattern does not match (e.g. a
        # non-numeric toolchain version); only keep a clean dotted-numeric version.
        [[ "${ver}" == *[^0-9.]* ]] && continue
        [[ -z "${ver}" ]] && continue
        # Strip any trailing dot for a stable numeric comparison.
        ver="${ver%%.}"
        # Pick this candidate if it's the first or strictly newer than the best
        # so far (dotted-numeric comparison; non-numeric fields are treated as 0).
        if [[ -z "${best}" ]]; then
            best="${candidate}"; bestver="${ver}"
            continue
        fi
        a="${ver}"; b="${bestver}"
        newer=0
        while [[ -n "${a}" || -n "${b}" ]]; do
            ia="${a%%.*}"; ib="${b%%.*}"
            [[ -z "${ia}" ]] && ia="0"
            [[ -z "${ib}" ]] && ib="0"
            [[ "${ia}" == *[^0-9]* ]] && ia="0"
            [[ "${ib}" == *[^0-9]* ]] && ib="0"
            if (( ia > ib )); then newer=1; break; fi
            if (( ia < ib )); then break; fi
            a="${a#*.}"; b="${b#*.}"
            [[ "${a}" == "${b}" ]] && a="" && b=""
        done
        if (( newer )); then
            best="${candidate}"; bestver="${ver}"
        fi
    done
    [[ -n "${best}" ]] || return 1
    printf '%s' "${best}"
}

# From the easyconfig for <soft> on <tc>, print "<software_version> <toolchain_version>".
# When the optional <required> arg is non-empty and the software cannot be
# resolved, prints a diagnostic (nonzero return) so required callers can abort;
# probe callers omit <required> to fail silently.
ec_versions() {
    local soft="$1" tc="$2" required="${3:-}" path base softver tcver
    path="$(ec_path "${soft}" "${tc}")" || {
        [[ -n "${required}" ]] && echo "error: could not resolve the ${soft} version on ${tc} from the EasyBuild easyconfigs tree" >&2
        return 1
    }
    base="$(basename "${path}")"
    softver="$(printf '%s' "${base}" | sed -E "s/^${soft}-([0-9][0-9.]*)-${tc}-([0-9][0-9.]*)\\.eb$/\\1/")"
    tcver="$(printf '%s' "${base}" | sed -E "s/^${soft}-([0-9][0-9.]*)-${tc}-([0-9][0-9.]*)\\.eb$/\\2/")"
    if [[ "${softver}" != "${base}" && "${tcver}" != "${base}" ]]; then
        printf '%s %s' "${softver}" "${tcver}"
        return 0
    fi
    [[ -n "${required}" ]] && echo "error: could not resolve the ${soft} version on ${tc} from the EasyBuild easyconfigs tree" >&2
    return 1
}

# Print the names of the extensions shipped inside a bundle easyconfig (e.g.
# the Python-bundle-PyPI or SciPy-bundle exts_list). The easyconfig file path is
# resolved directly from the EasyBuild easyconfigs tree; its exts_list exts are
# parsed from the file. Each ext name is printed on its own line. Prints a
# diagnostic and returns 1 if the easyconfig cannot be resolved or read (the
# callers abort on failure).
bundle_exts() {
    local soft="$1" tc="$2"
    local path
    path="$(ec_path "${soft}" "${tc}")" || {
        echo "error: could not resolve the ${soft} easyconfig on ${tc} from the EasyBuild easyconfigs tree" >&2
        return 1
    }
    if [[ ! -r "${path}" ]]; then
        echo "error: could not read the ${soft} exts_list from the EasyBuild easyconfigs tree" >&2
        return 1
    fi

    # Extract the exts_list body, then turn each tuple's opening `(` into a new
    # line so the first element becomes the leading token of its own line. Accept
    # only names whose next element looks like a version (digit-led), which
    # distinguishes real exts from e.g. `('md5', 'hash')` checksum tuples inside
    # the source dict.
    #
    # A real ext tuple is ('name', '<version>', ...) where the version contains a
    # decimal point. Requiring a dot in the second element excludes checksum
    # tuples such as ('md5', 'a1..') that live inside the source dict. Emit the
    # bare name (no surrounding quotes).
    printf '%s\n' "$(sed -n '/^[[:space:]]*exts_list[[:space:]]*=[[:space:]]*\[/,/^[[:space:]]*\][[:space:]]*$/p' "${path}")" \
        | tr '(' '\n' | sed -n "s/^[[:space:]]*'\([^']*\)',[[:space:]]*'[0-9][0-9.]*\.[0-9][^']*'.*/\1/p"
}

# Return 0 if the first argument is one of the newline-separated items in $2,
# else 1.
in_set() {
    [[ $'\n'"${2}"$'\n' == *$'\n'"${1}"$'\n'* ]]
}

# ---- Resolve toolchain + module versions from the easyconfigs tree ------------
# Each `ec_versions` call returns "<software_version> <toolchain_version>" for
# the software's easyconfig on its toolchain. From Python we get both the Python
# version and the GCCcore version beneath it; from Python-bundle-PyPI the bundle
# version; from SciPy-bundle both the bundle version and the gfbf toolchain
# version.
pair="$(ec_versions "Python" "GCCcore" 1)" || exit 1
py_version="${pair%% *}"

pair="$(ec_versions "Python-bundle-PyPI" "GCCcore" 1)" || exit 1
pypi_bundle_version="${pair%% *}"

pair="$(ec_versions "SciPy-bundle" "${TOOLCHAIN_NAME}" 1)" || exit 1
scipy_bundle_version="${pair%% *}"
toolchain_version="${pair#* }"

version="$(yq -r '.project.version' "${PYPROJECT}")"
description="$(yq -r '.project.description' "${PYPROJECT}")"

# EasyBuild software name: capitalise components of the distribution name.
name="$(yq -r '.project.name' "${PYPROJECT}")"
eb_name="$(printf '%s' "${name}" | awk -F'-' '{ for (i=1;i<=NF;i++) printf "%s%s", toupper(substr($i,1,1)), substr($i,2) }')"

# docurls: ordered, de-duplicated URLs from [project.urls].
urls="$(yq -r '.project.urls | to_entries[] | [.value, .key] | @tsv' "${PYPROJECT}")"
docurls=()
while IFS=$'\t' read -r value key; do
    [[ -z "${value}" ]] && continue
    case " ${docurls[*]:-} " in
        *" ${value} "*) : ;;
        *) docurls+=("${value}") ;;
    esac
done <<< "${urls}"

scripts="$(yq -r '.project.scripts | keys | .[]' "${PYPROJECT}")"

# ---- Collect runtime deps (report extras always included) ---------------------
# uvloop is macOS + CPython only; the recipe targets Linux via EasyBuild, so it
# is dropped directly from the dependency list.
deps=()
while IFS= read -r req; do
    [[ -z "${req}" ]] && continue
    name_only="$(printf '%s' "${req}" | cut -d';' -f1 | cut -d'=' -f1 | xargs)"
    [[ "${name_only}" == "uvloop" ]] && continue
    deps+=("${req}")
done < <(yq -r '.project.dependencies[]' "${PYPROJECT}")
while IFS= read -r req; do
    [[ -z "${req}" ]] && continue
    deps+=("${req}")
done < <(yq -r '.project."optional-dependencies".report[]' "${PYPROJECT}")

# ---- Route each dependency ----------------------------------------------------
# A dependency is either provided by the Python-bundle-PyPI module, by the
# SciPy-bundle module, by a standalone module on the target toolchain, or is a
# bare Python extension to be installed into Streamflow's own bundle (EXTS).

BUNDLE_EXTS="$(bundle_exts "Python-bundle-PyPI" "GCCcore")" || exit 1
SCIPY_BUNDLE_EXTS="$(bundle_exts "SciPy-bundle" "${TOOLCHAIN_NAME}")" || exit 1

exts=()
relax=()
module_deps=()

for req in ${deps[@]+"${deps[@]}"}; do
    name_part="$(printf '%s' "${req}" | cut -d';' -f1 | cut -d'=' -f1 | xargs)"
    version_part="$(printf '%s' "${req}" | sed -n 's/^[^=]*==\([^;]*\).*/\1/p' | xargs)"

    if in_set "${name_part}" "${BUNDLE_EXTS}"; then
        # Provided by the Python-bundle-PyPI module (listed in its exts_list).
        relax+=("${name_part}")
    elif in_set "${name_part}" "${SCIPY_BUNDLE_EXTS}"; then
        # Provided by the SciPy-bundle module (listed in its exts_list).
        module_deps+=("SciPy-bundle|${scipy_bundle_version}")
        relax+=("${name_part}")
    elif pair="$(ec_versions "${name_part}" "GCCcore")"; then
        # A standalone module easyconfig for this software on the target toolchain.
        ver="${pair%% *}"
        module_deps+=("${name_part}|${ver}")
        relax+=("${name_part}")
    else
        # A bare Python extension to install into this bundle.
        exts+=("${name_part}|${version_part}")
    fi
done

# ---- dependencies section ------------------------------------------------------
dependencies=("Python|${py_version}" "Python-bundle-PyPI|${pypi_bundle_version}")
seen_entries=""
for entry in ${module_deps[@]+"${module_deps[@]}"}; do
    mod="${entry%%|*}"; mver="${entry#*|}"
    case "|${seen_entries}|" in
        *"|${mod}|${mver}|"*) : ;;
        *) seen_entries+="|${mod}|${mver}"; dependencies+=("${entry}") ;;
    esac
done

# ---- preinstallopts -------------------------------------------------------------
preinstallopts=""
cmds=()
if [[ "${#relax[@]}" -gt 0 ]]; then
    for r in ${relax[@]+"${relax[@]}"}; do
        cmds+=("sed -i -e 's/\"${r}==[^\"]*\"/\"${r}\"/g' %(builddir)s/${name}-*")
    done
    for c in ${cmds[@]+"${cmds[@]}"}; do
        if [[ "${preinstallopts}" != "" ]]; then
            preinstallopts+=" && "
        fi
        preinstallopts+="${c}"
    done
fi

# ---- Build remaining conditional snippets --------------------------------------
# The list sections (docurls, dependencies, exts_list, sanity_check_commands,
# upstream_contacts) are emitted inline in the `cat` template via flat `printf`
# over each array, or as static lines when the value is a single plain variable.
# `name|version` entries are expanded with every `|` replaced by a space so the
# unquoted expansion word-splits into ordered `name version` arguments that
# `printf` pairs; single-element arrays use a second `printf` to emit one token
# per line (safe: no whitespace in tokens). The `printf` format carries the
# leading per-line indentation directly. Only the genuinely conditional
# fragment (preinstallopts) is precomputed.
if [[ -n "${preinstallopts}" ]]; then
    preinstallopts_block="preinstallopts = \"\"\"${preinstallopts}\"\"\""
else
    preinstallopts_block="preinstallopts = ''"
fi

# ---- render --------------------------------------------------------------------
cat <<EOF
easyblock = 'PythonBundle'

name = '${eb_name}'
version = '${version}'

homepage = '$(yq -r '.project.urls.Homepage // ""' "${PYPROJECT}")'
description = '${description}'
docurls = [
$(printf "    '%s',\n" $(printf '%s\n' "${docurls[@]}"))
]
github_account = 'alpha-unito'
software_license = Unknown
software_license_urls = ['https://www.gnu.org/licenses/lgpl-3.0.html']
upstream_contacts = ['$(yq -r '.project.authors[0].email // ""' "${PYPROJECT}")']
whatis = ['${description}']

toolchain = {'name': '${TOOLCHAIN_NAME}', 'version': '${toolchain_version}'}

dependencies = [
$(printf "    ('%s', '%s'),\n" ${dependencies[@]//|/ })
]

${preinstallopts_block}
installopts = '--no-deps'

exts_list = [
$(printf "    ('%s', '%s'),\n" ${exts[@]//|/ })
    ('${eb_name}', '${version}', {
        'sources': {
            'source_urls': ['https://github.com/alpha-unito/${name}/archive/refs/tags/'],
            'filename': 'v${version}.tar.gz',
        },
    }),
]

sanity_check_commands = [
$(printf "    ('%s', '--version'),\n" $(printf '%s\n' "${scripts}"))
]

moduleclass = 'tools'
EOF


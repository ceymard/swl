import * as XLSX from 'xlsx'
import {
  Chunk,
  Sequence,
  make_read_creator,
  s,
  URI,
  OPT_OBJECT,
  ParserType,
  Source,
  register,
  Sink
} from 'swl'


export type Selector = boolean | string


function get_stream(stream: NodeJS.ReadableStream): Promise<Buffer> {
  var accept: (b: Buffer) => void, reject: (e: any) => void
  const p = new Promise<Buffer>((_acc, _rej) => {
    accept = _acc
    reject = _rej
  })
  var buffers: Buffer[] = [];

  stream.on('data', function(data) { buffers.push(data); });
  stream.on('error', function (err) {
    reject(err)
  })
  stream.on('end', function() {
    var buffer = Buffer.concat(buffers);
    accept(buffer)
  });

  return p
}

const _l = ['', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
const columns: string[] = []

for (let i = 0; i < _l.length; i++) {
  for (let j = 1; j < _l.length; j++) {
    columns.push(_l[i] + _l[j])
  }
}


const XLS_OPTIONS = s.object({
  header: s.string()
})
const XLS_SOURCE_BODY = Sequence(URI, OPT_OBJECT)

@register('xls', '.xls', '.xlsx', '.xlsb', '.xlsm', '.ods')
export class XlsSource extends Source<
  s.BaseType<typeof XLS_OPTIONS>,
  ParserType<typeof XLS_SOURCE_BODY>
> {
  help = `Read collections from a notebook`
  body_parser = XLS_SOURCE_BODY
  options_parser = XLS_OPTIONS

  async emit() {
    const files = await make_read_creator(await this.body[0], {})

    for (var file of files) {
      const b = await get_stream(file.source)
      const w = XLSX.read(b, {cellHTML: false, cellText: false})
      await this.handleWorkbook(w)
    }
  }

  async handleWorkbook(w: XLSX.WorkBook) {
    for (var sname of w.SheetNames) {
      const s = w.Sheets[sname]
      const sources = this.body[1]

      // Find out if this sheet should be part of the extraction
      if (sources && Object.keys(sources).length > 0 && !sources[sname])
        // If there was a specification of keys and this sheet name is not
        // one of it, then just continue to the next collection
        continue

      const re_range = /^([A-Z]+)(\d+):([A-Z]+)(\d+)$/
      const match = re_range.exec(s['!ref'] as string)
      if (!match) continue

      // Try to figure out if we were given a header position globally
      // or for this specific sheet
      var header_line = 1
      var header_column = 0

      const re_header = /^([A-Z]+)(\d+)$/
      const hd = this.options.header || sources && sources![sname]
      if (typeof hd === 'string') {
        var m = re_header.exec(hd)
        if (m) {
          header_column = columns.indexOf(m[1])
          header_line = parseInt(m[2])
        }
      }
      // We have to figure out the number of lines
      const lines = parseInt(match[4])

      // Then we want to find the header row. By default it should be
      // "A1", or the first non-empty cell we find
      const header: string[] = []
      for (var i = header_column; i < columns.length; i++) {
        const cell = s[`${columns[i]}${header_line}`]
        if (!cell || !cell.v)
        break
        header.push(cell.v)
      }

      // Now that we've got the header, we just go on with the rest of the lines
      var not_found_count = 0
      for (var j = header_line + 1; j <= lines; j++) {
        var obj: {[name: string]: any} = {}
        var found = false
        for (i = header_column; i < header.length; i++) {
          const cell = s[`${columns[i]}${j}`]
          if (cell) {
            obj[header[i - header_column]] = cell.v
            found = true
            not_found_count = 0
          } else {
            obj[header[i - header_column]] = null
          }
        }

        if (found)
          await this.send(Chunk.data(sname, obj))
        else {
          not_found_count++
          if (not_found_count > 5)
          // More than five empty rows in a row means we're at the end of the document.
            break
        }
      }
    }
  }
}


const XLS_WRITE_OPTIONS = s.object({
  compression: s.boolean(false)
})

@register('xls', '.xls', '.xlsx', '.xlsb', '.xlsm', '.ods')
export class XlsSink extends Sink<
  s.BaseType<typeof XLS_WRITE_OPTIONS>,
  ParserType<typeof URI>
> {
  help = `Write collections to a workbook`
  options_parser = XLS_WRITE_OPTIONS
  body_parser = URI

  wb!: XLSX.WorkBook
  acc = [] as any[]

  async init() {
    this.wb = XLSX.utils.book_new()
  }

  async onCollectionEnd(name: string) {
    XLSX.utils.book_append_sheet(
      this.wb,
      XLSX.utils.json_to_sheet(this.acc),
      name
    )

    this.acc = []
  }

  async onData(chunk: Chunk.Data) {
    this.acc.push(chunk.payload)
  }

  async end() {
    XLSX.writeFile(this.wb, await this.body)
  }
}

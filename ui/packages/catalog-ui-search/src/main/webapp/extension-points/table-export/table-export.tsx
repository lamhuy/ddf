/**
 * Copyright (c) Codice Foundation
 *
 * This is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details. A copy of the GNU Lesser General Public License
 * is distributed along with this program and can be found at
 * <http://www.gnu.org/licenses/lgpl.html>.
 *
 **/
import * as React from 'react'
import TableExport from '../../react-component/container/table-export'
import {
  getExportOptions,
  Transformer,
} from '../../react-component/utils/export'

type Option = {
  label: string
  value: string
}

type ExportResponse = {
  displayName: string
  id: string
}

type Props = {
  selectionInterface: any
}

type State = {
  formats: Option[]
}

class TableExports extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props)
    this.state = { formats: [] }
  }
  componentDidMount = async () => {
    const exportFormats = await getExportOptions(Transformer.Query)
    const sortedExportFormats = exportFormats.sort(
      (format1: ExportResponse, format2: ExportResponse) => {
        return format1.displayName.localeCompare(format2.displayName)
      }
    )
    const formats = sortedExportFormats.map((exportFormat: ExportResponse) => ({
      label: exportFormat.displayName,
      value: exportFormat.id,
    }))
    this.setState({ formats })
  }
  render() {
    return (
      <TableExport
        exportFormats={this.state.formats}
        selectionInterface={this.props.selectionInterface}
      />
    )
  }
}

export default TableExports

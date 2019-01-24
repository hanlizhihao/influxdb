// Libraries
import React, {PureComponent} from 'react'
// Components
import {MultiInputType, MultipleInput} from 'src/clockface'
// Actions
import {setConfigArrayValue} from 'src/onboarding/actions/dataLoaders'
// Types
import {ConfigFieldType, TelegrafPluginName} from 'src/types/v2/dataLoaders'

interface Props {
  fieldName: string
  fieldType: ConfigFieldType
  addTagValue: (item: string, fieldName: string) => void
  removeTagValue: (item: string, fieldName: string) => void
  autoFocus: boolean
  value: string[]
  helpText: string
  onSetConfigArrayValue: typeof setConfigArrayValue
  telegrafPluginName: TelegrafPluginName
}

class ArrayFormElement extends PureComponent<Props> {
  public render() {
    const {fieldName, autoFocus, helpText} = this.props

    return (
      <div className="multiple-input-index">
        <MultipleInput
          title={fieldName}
          helpText={helpText}
          inputType={this.inputType}
          autoFocus={autoFocus}
          onAddRow={this.handleAddRow}
          onDeleteRow={this.handleRemoveRow}
          onEditRow={this.handleEditRow}
          tags={this.tags}
        />
      </div>
    )
  }

  private get inputType(): MultiInputType {
    switch (this.props.fieldType) {
      case ConfigFieldType.Uri:
      case ConfigFieldType.UriArray:
        return MultiInputType.URI
      case ConfigFieldType.String:
      case ConfigFieldType.StringArray:
        return MultiInputType.String
    }
  }

  private handleAddRow = (item: string) => {
    this.props.addTagValue(item, this.props.fieldName)
  }

  private handleRemoveRow = (item: string) => {
    const {removeTagValue, fieldName} = this.props

    removeTagValue(item, fieldName)
  }

  private handleEditRow = (index: number, item: string) => {
    const {onSetConfigArrayValue, telegrafPluginName, fieldName} = this.props

    onSetConfigArrayValue(telegrafPluginName, fieldName, index, item)
  }

  private get tags(): Array<{name: string; text: string}> {
    const {value} = this.props
    return value.map(v => {
      return {text: v, name: v}
    })
  }
}

export default ArrayFormElement

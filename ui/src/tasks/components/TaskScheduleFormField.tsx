// Libraries
import React, {ChangeEvent, PureComponent} from 'react'
// Components
import {Columns, Form, Grid, Input, InputType} from 'src/clockface'
// Types
import {TaskSchedule} from 'src/utils/taskOptionsToFluxScript'

interface Props {
  schedule: TaskSchedule
  cron: string
  offset: string
  interval: string
  onChangeInput: (e: ChangeEvent<HTMLInputElement>) => void
}

export default class TaskScheduleFormFields extends PureComponent<Props> {
  public render() {
    const {offset, onChangeInput, interval, cron, schedule} = this.props

    return (
      <>
        <Grid.Column widthXS={Columns.Six}>
          <Form.Element
            label={schedule === TaskSchedule.interval ? 'Every' : 'Cron'}
          >
            <Input
              name={schedule}
              type={InputType.Text}
              placeholder={
                schedule === TaskSchedule.interval ? '1d3h30s' : '0 2 * * *'
              }
              value={schedule === TaskSchedule.interval ? interval : cron}
              onChange={this.props.onChangeInput}
            />
          </Form.Element>
        </Grid.Column>

        <Grid.Column widthXS={Columns.Six}>
          <Form.Element label="Offset">
            <Input
              name="offset"
              type={InputType.Text}
              value={offset}
              placeholder="20m"
              onChange={onChangeInput}
            />
          </Form.Element>
        </Grid.Column>
      </>
    )
  }
}
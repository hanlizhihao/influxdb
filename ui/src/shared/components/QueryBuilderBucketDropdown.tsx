// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'
// Components
import {ComponentSize, Dropdown} from 'src/clockface'
// Actions
import {selectBucket} from 'src/shared/actions/v2/queryBuilder'
// Utils
import {getActiveQuery, getActiveTimeMachine,} from 'src/shared/selectors/timeMachines'
import {toComponentStatus} from 'src/shared/utils/toComponentStatus'
// Types
import {AppState} from 'src/types/v2'
import {RemoteDataState} from 'src/types'

interface StateProps {
  selectedBucket: string
  buckets: string[]
  bucketsStatus: RemoteDataState
}

interface DispatchProps {
  onSelectBucket: (bucket: string, resetSelections: boolean) => void
}

interface OwnProps {}

type Props = StateProps & DispatchProps & OwnProps

const QueryBuilderBucketDropdown: SFC<Props> = props => {
  const {selectedBucket, buckets, bucketsStatus, onSelectBucket} = props

  return (
    <Dropdown
      selectedID={selectedBucket}
      onChange={bucket => onSelectBucket(bucket, true)}
      buttonSize={ComponentSize.Small}
      status={toComponentStatus(bucketsStatus)}
    >
      {buckets.map(bucket => (
        <Dropdown.Item key={bucket} id={bucket} value={bucket}>
          {bucket}
        </Dropdown.Item>
      ))}
    </Dropdown>
  )
}

const mstp = (state: AppState) => {
  const {buckets, bucketsStatus} = getActiveTimeMachine(state).queryBuilder
  const selectedBucket =
    getActiveQuery(state).builderConfig.buckets[0] || buckets[0]

  return {selectedBucket, buckets, bucketsStatus}
}

const mdtp = {
  onSelectBucket: selectBucket,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(QueryBuilderBucketDropdown)

// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'
// Components
import {Grid} from 'src/clockface'
import Affixes from 'src/shared/components/view_options/options/Affixes'
import DecimalPlacesOption from 'src/shared/components/view_options/options/DecimalPlaces'
import ThresholdList from 'src/shared/components/view_options/options/ThresholdList'
import ThresholdColoring from 'src/shared/components/view_options/options/ThresholdColoring'
// Actions
import {setColors, setDecimalPlaces, setPrefix, setSuffix,} from 'src/shared/actions/v2/timeMachines'
// Utils
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'
// Constants
import {THRESHOLD_TYPE_BASE} from 'src/shared/constants/thresholds'
// Types
import {AppState, NewView} from 'src/types/v2'
import {DecimalPlaces, SingleStatView} from 'src/types/v2/dashboards'
import {Color, ThresholdConfig} from 'src/types/colors'

interface StateProps {
  colors: Color[]
  prefix: string
  suffix: string
  decimalPlaces: DecimalPlaces
}

interface DispatchProps {
  onSetPrefix: typeof setPrefix
  onSetSuffix: typeof setSuffix
  onSetDecimalPlaces: typeof setDecimalPlaces
  onSetColors: typeof setColors
}

type Props = StateProps & DispatchProps

const SingleStatOptions: SFC<Props> = props => {
  const {
    colors,
    prefix,
    suffix,
    decimalPlaces,
    onSetPrefix,
    onSetSuffix,
    onSetDecimalPlaces,
    onSetColors,
  } = props

  const colorConfigs = colors.filter(c => c.type !== 'scale').map(color => {
    const isBase = color.id === THRESHOLD_TYPE_BASE

    const config: ThresholdConfig = {
      color,
      isBase,
    }

    if (isBase) {
      config.label = 'Base'
    }

    return config
  })

  return (
    <>
      <Grid.Column>
        <h4 className="view-options--header">Customize Single-Stat</h4>
      </Grid.Column>
      <Affixes
        prefix={prefix}
        suffix={suffix}
        onUpdatePrefix={onSetPrefix}
        onUpdateSuffix={onSetSuffix}
      />
      {decimalPlaces && (
        <DecimalPlacesOption
          digits={decimalPlaces.digits}
          isEnforced={decimalPlaces.isEnforced}
          onDecimalPlacesChange={onSetDecimalPlaces}
        />
      )}
      <Grid.Column>
        <h4 className="view-options--header">Colorized Thresholds</h4>
      </Grid.Column>
      <ThresholdList
        colorConfigs={colorConfigs}
        onUpdateColors={onSetColors}
        onValidateNewColor={() => true}
      />
      <Grid.Column>
        <ThresholdColoring />
      </Grid.Column>
    </>
  )
}

const mstp = (state: AppState) => {
  const view = getActiveTimeMachine(state).view as NewView<SingleStatView>
  const {colors, prefix, suffix, decimalPlaces} = view.properties

  return {colors, prefix, suffix, decimalPlaces}
}

const mdtp: DispatchProps = {
  onSetPrefix: setPrefix,
  onSetSuffix: setSuffix,
  onSetDecimalPlaces: setDecimalPlaces,
  onSetColors: setColors,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(SingleStatOptions)

<?php
declare(strict_types=1);

namespace App\Model\Entity;

use Cake\ORM\Entity;

/**
 * IndicadoresFundo Entity
 *
 * @property int $cnpj_fundo_id
 * @property int $periodo_meses
 * @property \Cake\I18n\FrozenDate $data_final
 * @property string $rentabilidade
 * @property string $desvio_padrao
 * @property int|null $num_valores
 * @property string|null $rentab_min
 * @property string|null $rentab_max
 * @property string $max_drawdown
 * @property int|null $tipo_benchmark_id
 * @property int|null $meses_acima_bench
 * @property string|null $sharpe
 * @property string|null $beta
 *
 * @property \App\Model\Entity\CnpjFundo $cnpj_fundo
 * @property \App\Model\Entity\TipoBenchmark $tipo_benchmark
 */
class IndicadoresFundo extends Entity
{
    /**
     * Fields that can be mass assigned using newEntity() or patchEntity().
     *
     * Note that when '*' is set to true, this allows all unspecified fields to
     * be mass assigned. For security purposes, it is advised to set '*' to false
     * (or remove it), and explicitly make individual fields accessible as needed.
     *
     * @var array
     */
    protected $_accessible = [
        'rentabilidade' => true,
        'desvio_padrao' => true,
        'num_valores' => true,
        'rentab_min' => true,
        'rentab_max' => true,
        'max_drawdown' => true,
        'tipo_benchmark_id' => true,
        'meses_acima_bench' => true,
        'sharpe' => true,
        'beta' => true,
        'cnpj_fundo' => true,
        'tipo_benchmark' => true,
    ];
}

<?php
declare(strict_types=1);

namespace App\Model\Entity;

use Cake\ORM\Entity;

/**
 * RetornoRiscoFundo Entity
 *
 * @property int $cnpj_fundo_id
 * @property int $periodo_meses
 * @property \Cake\I18n\FrozenDate $data_final
 * @property float $rentab_media
 * @property float $desvio_padrao
 * @property int|null $num_valores
 * @property float|null $rentab_min
 * @property float|null $rentab_max
 * @property int|null $meses_abaixo_bench
 * @property int|null $meses_acima_bench
 *
 * @property \App\Model\Entity\CnpjFundo $cnpj_fundo
 */
class RetornoRiscoFundo extends Entity
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
        'cnpj_fundo_id' => true,
        'periodo_meses' => true,
        'data_final' => true,
        'rentab_media' => true,
        'desvio_padrao' => true,
        'num_valores' => true,
        'rentab_min' => true,
        'rentab_max' => true,
        'meses_abaixo_bench' => true,
        'meses_acima_bench' => true,
        'cnpj_fundo' => true,
    ];
}

<?php
declare(strict_types=1);

namespace App\Model\Entity;

use Cake\ORM\Entity;

/**
 * CnpjFundo Entity
 *
 * @property int $id
 * @property string $CNPJ
 * @property string $DENOM_SOCIAL
 * @property \Cake\I18n\FrozenDate $DT_REG_CVM
 *
 * @property \App\Model\Entity\CadastroFundo[] $cadastro_fundos
 * @property \App\Model\Entity\CancelamentoFundo[] $cancelamento_fundos
 * @property \App\Model\Entity\DocExtratosFundo[] $doc_extratos_fundos
 * @property \App\Model\Entity\DocInfDiarioFundo[] $doc_inf_diario_fundos
 * @property \App\Model\Entity\IndicadoresFundo[] $indicadores_fundos
 * @property \App\Model\Entity\OperacoesFinanceira[] $operacoes_financeiras
 * @property \App\Model\Entity\SituacaoFundo[] $situacao_fundos
 */
class CnpjFundo extends Entity
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
        'CNPJ' => true,
        'DENOM_SOCIAL' => true,
        'DT_REG_CVM' => true,
        'cadastro_fundos' => true,
        'cancelamento_fundos' => true,
        'doc_extratos_fundos' => true,
        'doc_inf_diario_fundos' => true,
        'indicadores_fundos' => true,
        'operacoes_financeiras' => true,
        'situacao_fundos' => true,
    ];
}
